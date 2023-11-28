"""
Functions related to querying and managing objects in DNAnexus, as well
as running jobs.
"""
from copy import deepcopy
import concurrent.futures
import json
import os
import re
import sys
from time import sleep
from timeit import default_timer as timer
from typing import List, Tuple

import dxpy
from packaging.version import Version
import pandas as pd

from .utils import (
    check_exclude_samples,
    check_report_index,
    filter_manifest_samples_by_files,
    make_path,
    prettier_print
)

# for prettier viewing in the logs
pd.set_option('display.max_rows', 100)
pd.set_option('max_colwidth', 1500)


class DXManage():
    """
    Methods for generic handling of dx related things
    """
    def read_assay_config_file(self, file) -> dict:
        """
        Read assay config file specified with -iassay_config_file

        Parameters
        ----------
        file : str
            DNAnexus file ID of config to read

        Returns
        -------
        dict
            JSON files read into a dict
        """
        print("\n \nReading in specified assay config file...")
        contents = self.read_dxfile(file)
        config = json.loads('\n'.join(contents))

        # get the name of the file used for displaying in summary report
        file_details = dxpy.DXFile(
            re.match(r'file-[\d\w]+', file).group()
        ).describe()

        config['name'] = file_details['name']
        config['dxid'] = file_details['id']

        print("Assay config file contents:")
        prettier_print(config)

        return config


    def get_assay_config(self, path, assay) -> dict:
        """
        Get highest config file from given path for given assay and
        read in to a dict

        Parameters
        ----------
        path : str
            DNAnexus project:path to dir containing assay configs
        assay : str
            assay string to return configs for (i.e. CEN or WES)

        Returns
        -------
        dict
            contents of config file

        Raises
        ------
        AssertionError
            Raised if 'path' parameter not a valid project-xxx:/path
        AssertionError
            Raised if no config files found at the given path
        AssertionError
            Raised if no config files found for the given assay string
        """
        # searching dir for configs, check for valid project:path structure
        assert re.match(r'project-[\d\w]*:/.*', path), (
            f'path to assay configs appears invalid: {path}'
        )

        print(f"\n \nSearching following path for assay configs: {path}")

        project, project_path = path.split(':')

        files = list(dxpy.find_data_objects(
            name=".json$",
            name_mode='regexp',
            project=project,
            folder=project_path,
            describe=True
        ))

        # sense check we find config files
        assert files, f"No config files found in given path: {path}"

        files_ids='\n\t'.join([
            f"{x['describe']['name']} ({x['id']} - "
            f"{x['describe']['archivalState']})" for x in files])
        print(f"\nAssay config files found:\n\t{files_ids}")

        highest_config = {}

        for file in files:
            if not file['describe']['archivalState'] == 'live':
                print(
                    "Config file not in live state - will not be used: "
                    f"{file['describe']['name']} ({file['id']})"
                )
                continue

            config_data = json.loads(
                dxpy.DXFile(
                    project=file['project'],
                    dxid=file['id']
                ).read())

            if not config_data.get('assay') == assay:
                continue

            if Version(config_data.get('version')) > Version(highest_config.get('version', '0')):
                config_data['dxid'] = file['id']
                config_data['name'] = file['describe']['name']
                highest_config = config_data

        assert highest_config, (
            f"No config file was found for {assay} from {path}"
        )

        print(
            f"Highest version config found for {assay} was "
            f"{highest_config.get('version')} from {highest_config.get('dxid')}"
        )

        print("Assay config file contents:")
        prettier_print(highest_config)

        return highest_config


    def get_file_project_context(self, file) -> dxpy.DXObject:
        """
        Get project ID for a given file ID, used where only file ID is
        provided as DXFile().read() requires both, will ensure that
        only a live version of a project context is returned.

        Parameters
        ----------
        file : str
            file ID of file to search

        Returns
        -------
        DXObject
            DXObject file handler object

        Raises
        ------
        AssertionError
            Raised if no live copies of the given file could be found
        """
        print(f"Searching all projects for: {file}")

        file_details = dxpy.DXFile(dxid=file).describe()
        files = dxpy.find_data_objects(
            name=file_details['name'],
            describe=True
        )

        # filter out any archived files or those resolving
        # to the current job container context
        files = [
            x for x in files
            if x['describe']['archivalState'] == 'live'
            and not re.match(r"^container-[\d\w]+$", x['project'])
        ]
        assert files, f"No live files could be found for the ID: {file}"

        print(
            f"Found {file} in {len(files)} projects, "
            f"using {files[0]['project']} as project context"
        )

        return files[0]


    def find_files(
        self, path, subdir='', limit=None, pattern=None) -> List[dxpy.DXObject]:
        """
        Search given path in DNAnexus, optionally filter down by a sub
        directory and / or with a file name regex pattern. Default
        behaviour is to just return all files in the given path.

        Parameters
        ----------
        path : str
            path to where to search
        subdir : str (optional)
            sub directory to search, will partially match as /path/dir.*
        limit : integer (optional)
            no. of files to limit searching to
        pattern : str (optional)
            regex file pattern to search for

        Returns
        -------
        list
            list of files found
        """
        path = path.rstrip('/')
        if subdir:
            subdir = subdir.strip('/')

        print(
            f"Searching for files in {path} and subdir {subdir} with "
            f"pattern '{pattern}'"
        )

        project = re.search(r'project-[\d\w]+', path)
        if project:
            project = project.group()

        path = re.sub(r'^project-[\d\w]+:', '', path)

        files = list(dxpy.find_data_objects(
            name=pattern,
            name_mode='regexp',
            project=project,
            folder=path,
            limit=limit,
            describe=True
        ))

        if subdir:
            # filter down to just those in the given sub dir
            sub_path = f"{path}/{subdir}".lower()
            files = [
                x for x in files
                if x['describe']['folder'].lower().startswith(sub_path)
            ]

        not_live = [
            f"{x['describe']['name']} ({x['id']})" for x in files
            if x['describe']['archivalState'] != 'live'
        ]
        if not_live:
            print(
                "WARNING: some files found are in an archived state, if these "
                "are for samples to be analysed this will raise an error..."
            )
            prettier_print(not_live)

        print(f"Found {len(files)} files in {path}/{subdir}")

        return files


    def read_dxfile(self, file) -> List[str]:
        """
        Read contents of a DXFile object

        Abstracted method for reading files such as manifest and genepanels

        Parameters
        ----------
        file : str | dict
            file ID of DXFile to read, may be passed as 'file-xxx',
            'project-xxx:file-xxx' or {'$dnanexus_link': '[project-xxx:]file-xxx'}

        Returns
        -------
        list
            contents of file as a list split on '\n'

        Raises
        ------
        RuntimeError
            Raised if 'file' argument not in an expected format
        AssertionError
            Raised if project and file ID not correctly parsed
        """
        print(f"Reading from {file}")
        if not file:
            # None passed, not sure if I need to handle this but keep tripping
            # myself up with tests so just going to return and probably
            # end up moving the error along somewhere else but oh well
            print("Empty file passed to read_dxfile() :sadpepe:")
            return

        if isinstance(file, dict):
            # provided as {'$dnanexus_link': '[project-xxx:]file-xxx'}
            file = file.get('$dnanexus_link')

        if re.match(r'^file-[\d\w]+$', file):
            # just file-xxx provided => find a project context to use
            file_details = self.get_file_project_context(file)
            project = file_details.get('project')
            file_id = file_details.get('id')
        elif re.match(r'^project-[\d\w]+:file-[\d\w]+', file):
            # nicely provided as project-xxx:file-xxx
            project, file_id = file.split(':')
        else:
            # who knows what's happened, not for me to deal with
            raise RuntimeError(
                f"DXFile not in an expected format: {file}"
            )

        # sense check that we actually got a project and file ID from above
        assert all([project, file_id]), (
            "Missing project and / or file ID - "
            f"project: {project}, file: {file_id}"
        )

        return dxpy.DXFile(project=project, dxid=file_id).read().split('\n')


    def check_archival_state(self, files, unarchive, samples=None) -> None:
        """
        Check archival state of n files, to be used before attempting
        to launch jobs to ensure nothing fails due to archived files.

        Files may be in the following states:

        - live -> nothing to do
        - archival -> archiving has been requested but not yet happened
            and / or not all instances of the file have been archived =>
            can be unarchived
        - archived -> file fully archived, will take time to unarchive fully
        - unarchiving -> already requested unarchiving but not yet
            completed, skip trying to unarchive again

        Parameters
        ---------
        files : list
            list of DXFile objects to check state of
        unarchive : bool
            if to automatically unarchive files
        samples : list
            list of sample names to filter down files to check

        Raises
        ------
        RuntimeError
            Raised when one or more files found that are in state 'unarchiving'
            and unarchive=True specified
        RuntimeError
            Raised when required files are archived and -iunarchive=False
        """
        print(f"\n \nChecking archival state of {len(files)} files...")

        # find files not in a live state, and filter these down by samples
        # given that we're going to launch jobs for
        not_live = [
            x for x in files if x['describe']['archivalState'] != 'live'
        ]

        if samples and not_live:
            not_live_filtered = []
            for dx_file in not_live:
                match = False
                for name in samples:
                    if dx_file['describe']['name'].startswith(name):
                        match = True
                        break

                if match and dx_file not in not_live_filtered:
                    # this file is archived and in one of our samples
                    not_live_filtered.append(dx_file)

            not_live = not_live_filtered

        if not not_live:
            # nothing archived that we need :dancing_penguin:
            print("No required files in archived state")
            return

        # check for files currently unarchiving (i.e. in 'unarchiving') since
        # an error will be raised if we try unarchive these in this state
        unarchiving = []
        to_unarchive = []

        for file in not_live:
            if file['describe']['archivalState'] == 'unarchiving':
                unarchiving.append(file)
            else:
                to_unarchive.append(file)

        not_live_ids = ' '.join([x['id'] for x in not_live])
        not_live_printable = '\n\t'.join([
            f"{x['describe']['name']} ({x['id']}) - {x['describe']['archivalState']}"
            for x in not_live
        ])

        print(
            f"\n \nWARNING: {len(not_live)} sample files to use for analysis "
            f"are not in a live state:\n\t{not_live_printable}\n \n"
        )

        print(f"{len(unarchiving)} files are currently in state 'unarchiving'")
        print(f"{len(to_unarchive)} files are in state 'archived'")

        if unarchive:
            if not to_unarchive:
                # we have specified to unarchive, but all non-live files
                # are not in a state that can be unarchived (i.e. unarchiving
                # already requested) => raise error
                print(
                    "ERROR: -unarchive=true but all non-live files found "
                    "are not in an archived/archival state, and therefore "
                    "unarchiving can not be performed"
                )
                raise RuntimeError(
                    'non-live files not in a state that can be unarchived'
                )

            print(
                "\n \n-iunarchive=true specified, will start unarchiving...\n \n"
            )
            self.unarchive_files(to_unarchive)
        else:
            # not unarchiving => print a handy message and rage quit
            print(
                f"ERROR: files required are archived and -iunarchive not "
                f"specified, file IDs of archived files:\n\t{not_live_ids}"
            )
            raise RuntimeError('Files required for analysis archived')


    def unarchive_files(self, files) -> None:
        """
        Unarchive given file IDs ready for analysis, will set off unarchiving
        and terminate the app since unarchiving takes a while

        Parameters
        ----------
        files : list
            DXFile objects of files to unarchive

        Raises
        ------
        RuntimeError
            Raised if unarchiving fails after 5 attempts on a given file
        """
        for idx, dx_file in enumerate(files):
            print(
                f"[{idx+1}/{len(files)}] Unarchiving "
                f"{dx_file['describe']['name']} ({dx_file['id']})"
            )

            # add some buffer in case DNAnexus gets angry at lots of requests
            sleepy_time = 10
            unarchived = False

            for attempt in range(1, 6):
                try:
                    dxpy.DXFile(
                        project=dx_file['project'],
                        dxid=dx_file['id']
                    ).unarchive()
                except Exception as error:
                    print(
                        f"\n[Attempt {attempt}/5] Error in unarchiving file:\n"
                        f"\t{error}\n\nWaiting {sleepy_time}s to retry"
                    )
                    sleep(sleepy_time)
                    sleepy_time = sleepy_time * 2
                else:
                    unarchived = True
                    # add a sleep once unarchive request to keep DNAnexus
                    # happy since they don't seem to understand queuing
                    sleep(2)
                    break

                print(
                    f"[Attempt {attempt}/5] Error in unarchiving "
                    f"file: {dx_file['id']}"
                )

            if not unarchived:
                raise RuntimeError(
                    f"[Attempt {attempt}/5] Too many errors trying to "
                    f"unarchive file: {dx_file['id']}. Exiting."
                )

        # build a handy command to dump into the logs for people to check
        # the state of all of the files we're unarchiving later on
        check_state_cmd = (
            f"echo {' '.join([x['id'] for x in files])} | xargs -n1 -d' ' -P32 "
            "-I{} bash -c 'dx describe --json {} ' | grep archival | uniq -c"
        )

        print(
            f"\n \nUnarchiving requested for {len(files)} files, this "
            "will take some time...\n \n"
        )

        print(
            "The state of all files may be checked with the following command:"
            f"\n \n\t{check_state_cmd}\n \n"
        )

        print(
            "This job can be relaunched once unarchiving is complete by "
            "running:\n \n\tdx run app-eggd_dias_batch --clone "
            f"{os.environ.get('DX_JOB_ID')}"
        )

        # tag job to know its not launched any jobs
        dxpy.DXJob(dxid=os.environ.get('DX_JOB_ID')).add_tags(
            [f"Unarchiving of {len(files)} requested, no jobs launched"]
        )

        sys.exit(0)


    def format_output_folders(self, workflow, single_output, time_stamp) -> dict:
        """
        Generate dict of output folders for each stage of given workflow
        for passing to dxpy.DXWorkflow().run()

        Will be formatted as: {
            # for apps
            "stage-xxx":
                "/{single_output}/{workflow_name}/{timestamp}/{app_name}-{version}

            # for applets
            "stage-xxx":
                "/{single_output}/{workflow_name}/{timestamp}/{applet_name}
        }

        Parameters
        ----------
        workflow : dict
            describe output of workflow from dxpy.describe()
        single_output : str
            path to single output dir
        time_stamp : str
            time app launched to add to folder path

        Returns
        -------
        dict
            mapping of stage ID -> output folder path
        """
        print("\n \nGenerating output folder structure")
        stage_folders = {}

        for stage in workflow['stages']:
            if stage['executable'].startswith('applet-'):
                applet_details = dxpy.describe(stage['executable'])
                folder_name = applet_details['name']
            else:
                folder_name = stage['executable'].replace(
                    'app-', '', 1).replace('/', '-')

            path = make_path(
                single_output, workflow['name'], time_stamp, folder_name
            )

            stage_folders[stage['id']] = path

        print("Output folders to use:")
        prettier_print(stage_folders)

        return stage_folders


class DXExecute():
    """
    Methods for handling execution of apps / workflows
    """
    def cnv_calling(
            self,
            config,
            single_output_dir,
            exclude,
            start,
            wait,
            unarchive
        ) -> str:
        """
        Run CNV calling for given samples in manifest

        Parameters
        ----------
        config : dict
            dict of the assay config with inputs for cnv calling
        single_output_dir : str
            path to single output directory
        exclude : list
            list of sample IDs to exclude bam files from calling, will be
            formatted as InstrumentID-SpecimenID (i.e. [123245111-33202R00111, ...])
        start : str
            start time of running app for naming output folders
        wait : bool
            if to set hold_on_wait to wait on job to finish
        unarchive : bool
            controls if to automatically unarchive any archived files

        Returns
        -------
        str
            job ID of launch cnv calling job

        Raises
        ------
        dxpy.exceptions.DXJobFailureError
            Raised when CNV calling fails / terminates / timed out
        """
        print("\n \nBuilding inputs for CNV calling")
        cnv_config = config['modes']['cnv_call']

        # find BAM files and format as $dnanexus_link inputs to add to config
        bam_dir = make_path(
            single_output_dir, cnv_config['inputs']['bambais']['folder']
        )

        # check if we're searching for files in different project
        remote_project = re.match(r"project-[\w]+", single_output_dir)
        if remote_project:
            bam_dir = f"{remote_project.group()}:{bam_dir}"

        files = DXManage().find_files(
            pattern=cnv_config['inputs']['bambais']['name'],
            path=bam_dir
        )

        printable_files = '\n\t'.join([x['describe']['name'] for x in files])
        print(
            f"Found {len(files)} .bam/.bai files in {bam_dir}:"
            f"\n\t{printable_files}"
        )

        if exclude:
            # filtering out sample files specified from -iexclude
            samples = '\n\t'.join(exclude)
            print(f"Samples specified to exclude from CNV calling:\n\t{samples}")

            check_exclude_samples(
                samples=[x['describe']['name'] for x in files],
                exclude=exclude,
                mode='calling'
            )

            # get the files of samples we're not excluding
            files = [
                file for file in files
                if not any([
                    file['describe']['name'].startswith(x) for x in exclude
                ])
            ]

            printable_files = '\n\t'.join([x['describe']['name'] for x in files])
            print(
                f"{len(files)} .bam/.bai files after excluding:"
                f"\n\t{printable_files}"
            )

        # check to ensure all bams are unarchived
        DXManage().check_archival_state(files, unarchive=unarchive)

        files = [{"$dnanexus_link": file} for file in files]
        cnv_config['inputs']['bambais'] = files

        # set output folder relative to single dir
        app_details = dxpy.describe(config.get('cnv_call_app_id'))
        folder = make_path(
            single_output_dir,
            f"{app_details['name']}-{app_details['version']}",
            start
        )

        print(f"Running CNV calling, outputting to {folder}")

        job = dxpy.DXApp(dxid=config.get('cnv_call_app_id')).run(
            app_input=cnv_config['inputs'],
            project=os.environ.get('DX_PROJECT_CONTEXT_ID'),
            folder=folder,
            priority='high',
            detach=True,
            instance_type=cnv_config.get('instance_type')
        )

        job_id = job.describe().get('id')
        job_handle = dxpy.DXJob(dxid=job_id)

        if wait:
            print("Holding app until CNV calling completes...")
            try:
                # holds app until job returns success
                job_handle.wait_on_done()
            except dxpy.exceptions.DXJobFailureError as err:
                # dx job error raised (i.e. failed, timed out, terminated)
                raise dxpy.exceptions.DXJobFailureError(
                    f"CNV calling failed in job {job_id}:\n\n{err}"
                )
            print("CNV calling completed successfully\n")
        else:
            print(f'CNV calling launched: {job_id}\n')

        return job_id


    def reports_workflow(
            self,
            mode,
            workflow_id,
            single_output_dir,
            manifest,
            config,
            start,
            name_patterns,
            sample_limit=None,
            call_job_id=None,
            parent=None,
            unarchive=None,
            exclude=None
        ) -> Tuple[list, dict, dict]:
        """
        Run Dias reports (or CNV reports) workflow for either
        CNV,SNV or mosaic reports

        Parameters
        ----------
        mode : str
            str of [CNV | SNV | mosaic], controls if running reports on
            CNV calling output, mosaic (mutect2) output or SNVs
        workflow_id : str
            dxid of Dias reports workflow
        single_output_dir : str
            dnanexus path to Dias single output
        manifest : dict
            mapping of sampleID -> testCodes parsed from manifest
        config : dict
            subset of assay config file containing the inputs for the given
            mode being run (i.e. {'modes': {'cnv_reports': {...}})
        start : str
            start time of running app for naming output folders
        name_patterns : dict (optional)
            set of regex patterns for matching sample names against files
            etc. for each type of manifest (i.e. Epic -> ^[\d\w]+-[\d]\w]+[-_])
        sample_limit : int (optional)
            no. of samples to launch jobs for
        call_job_id : str (optional)
            job ID of CNV calling to use output from (for CNV reports)
        parent : list | None
            single item list of parent dias batch job ID to use when
            testing to stop jobs running, or None when not running in test
        unarchive : bool (optional)
            controls if to automatically unarchive any archived files
        exclude : list (optional)
            list of sample names to exclude from generating reports (n.b.
            this is ONLY for CNV reports), will be formatted as
            InstrumentID-SpecimenID (i.e. [123245111-33202R00111, ...])

        Returns
        -------
        list
            list of job IDs launched
        dict
            dict of any errors found (i.e samples with no files)
        dict
            dict of per sample summary of names used for jobs

        Raises
        ------
        RuntimeError
            Raised when name patterns not present in config file
        RuntimeError
            Raised when invalid mode set
        RuntimeError
            Raised when no samples left in manifest after filtering
            against returned files

        [mode : CNV]
        RuntimeError
            Raised when exclude intervals bed not found from CNV calling job
        RuntimeError
            Raised when VCFs could not be found from CNV calling job

        [mode : SNV|mosaic]
        RuntimeError
            Raised when VCFs could not be found in given directory
        RuntimeError
            Raised when mosdepth files could not be found in given directory
        """
        print(f"\n \nConfiguring inputs for {mode} reports")

        # find all previous xlsx reports to use for indexing report names
        print("\n \nSearching for previous xlsx reports")
        xlsx_reports = DXManage().find_files(
            path=single_output_dir,
            pattern=r".xlsx$"
        )
        xlsx_reports = [
            x['describe']['name'] for x in xlsx_reports
        ]
        if xlsx_reports:
            reports = '\n\t'.join(sorted(xlsx_reports))
            print(f"xlsx reports found:\n\t{reports}")


        # this will either be Epic, Gemini or both
        manifest_source = sorted(set([
            x['manifest_source'] for x in manifest.values()]))

        if manifest_source == ['Epic']:
            pattern = name_patterns.get('Epic')
            manifest_source = 'Epic'
        elif manifest_source == ['Gemini']:
            pattern = name_patterns.get('Gemini')
            manifest_source = 'Gemini'
        elif manifest_source == ['Epic', 'Gemini']:
            # got 2 (or more) manifests with a mix => use both
            pattern = (
                fr"{name_patterns.get('Gemini')}|{name_patterns.get('Epic')}"
            )
            manifest_source = 'Epic&Gemini'
        else:
            # who knows what happens if we got here
            raise RuntimeError(
                f'Unable to correctly parse manifest source. Parsed: {manifest_source}'
            )

        vcf_files = []
        mosdepth_files = []
        excluded_intervals_bed_file = []

        # gather errors to display in summary report
        errors = {}

        # set up required files for each running mode
        if mode == 'CNV':
            # get required files
            job_details = dxpy.DXJob(dxid=call_job_id).describe()

            vcf_input_field = 'stage-cnv_vep.vcf'

            vcf_dir = f"{job_details.get('project')}:{job_details.get('folder')}"
            vcf_name = config.get('inputs').get(vcf_input_field).get('name')

            print('\n \nSearching for excluded intervals bed file')
            excluded_intervals_bed_file = list(DXManage().find_files(
                path=f"{job_details.get('project')}:{job_details.get('folder')}",
                pattern="_excluded_intervals.bed$",
                limit=1
            ))

            if not excluded_intervals_bed_file:
                raise RuntimeError(
                    f"Failed to find excluded intervals bed file from {call_job_id}"
            )

            excluded_intervals_bed = {
                "$dnanexus_link": {
                    "project": excluded_intervals_bed_file[0]['project'],
                    "id": excluded_intervals_bed_file[0]['id']
                }
            }

            print("\n \nSearching for VCF files")
            vcf_files = list(DXManage().find_files(
                path=vcf_dir,
                pattern=vcf_name
            ))

            if not vcf_files:
                raise RuntimeError(
                    f"Failed to find vcfs from {call_job_id} ({vcf_dir})"
            )

            print(
                "VCFs found:\n\t", '\n\t'.join(
                    sorted([x['describe']['name'] for x in vcf_files])
                )
            )

            if exclude:
                # exclude samples specified and won't have been through CNV
                # calling => exclude trying to launch CNV reports for these
                check_exclude_samples(
                    samples=manifest.keys(),
                    exclude=exclude,
                    mode='reports'
                )

                excluded = [
                    sample for sample in manifest.keys() if sample in exclude
                ]

                manifest = {
                    sample: config for sample, config in manifest.items()
                    if sample not in excluded
                }

                print(
                    f"\n \nSamples specified to exclude: {exclude}\nExcluded "
                    f"{len(excluded)} samples from manifest: {excluded}\n"
                    "Total samples left in manifest to launch CNV reports "
                    f"workflows for: {len(manifest.keys())}"
                )

            print(
                f"\n \nFound {len(vcf_files)} segments.vcf files from "
                f"{job_details.get('folder')} and {len(xlsx_reports)} "
                f"previous xlsx reports"
            )

        elif mode in ('SNV', 'mosaic'):
            vcf_input_field = 'stage-rpt_vep.vcf'

            vcf_dir = config.get('inputs').get(vcf_input_field).get('folder')
            vcf_name = config.get('inputs').get(vcf_input_field).get('name')

            mosdepth_dir = config.get('inputs').get(
                'stage-rpt_athena.mosdepth_files').get('folder')
            mosdepth_name = config.get('inputs').get(
                'stage-rpt_athena.mosdepth_files').get('name')

            print("\n \nSearching for VCF files")

            vcf_files = DXManage().find_files(
                path=single_output_dir,
                subdir=vcf_dir,
                pattern=vcf_name
            )

            if not vcf_files:
                error = (
                    f"Found no vcf files! {mode} reports in {single_output_dir} "
                    f"and subdir {vcf_dir} with pattern {vcf_name}"
                )

                raise RuntimeError(error)

            print(
                "VCFs found:\n\t", '\n\t'.join(
                    sorted([x['describe']['name'] for x in vcf_files])
                )
            )

            print("\n \nSearching for mosdepth files")
            mosdepth_files = DXManage().find_files(
                path=single_output_dir,
                subdir=mosdepth_dir,
                pattern=mosdepth_name
            )

            if not mosdepth_files:
                error = (
                    f"Found no mosdepth files! {mode} reports in "
                    f"{single_output_dir} and subdir {mosdepth_dir} with "
                    f"pattern {mosdepth_name}"
                )

                raise RuntimeError(error)

            manifest, _, manifest_no_mosdepth = filter_manifest_samples_by_files(
                manifest=manifest,
                files=mosdepth_files,
                name='mosdepth',
                pattern=pattern
            )

            if manifest_no_mosdepth:
                errors[
                    f"Samples in manifest with no mosdepth files found "
                    f"({len(manifest_no_mosdepth)})"
                ] = manifest_no_mosdepth

            print(
                f"Found {len(vcf_files)} vcf files from "
                f"{single_output_dir} in subdir {vcf_dir}, "
                f"{len(mosdepth_files)} from {single_output_dir} in subdir "
                f"{mosdepth_dir} and {len(xlsx_reports)} previous xlsx reports"
            )

        else:
            # this really shouldn't happen as we call it, but catch it
            # incase I forget and do something dumb (which is likely)
            raise RuntimeError(
                f"Invalid mode set for running reports: {mode}"
            )


        # ensure we have a vcf per sample, exclude those that don't have one
        manifest, manifest_no_match, manifest_no_vcf = \
            filter_manifest_samples_by_files(
                manifest=manifest,
                files=vcf_files,
                name='vcf',
                pattern=pattern
            )

        if manifest_no_match:
            errors[
                f"Samples in manifest not matching expected {manifest_source} "
                f"pattern ({len(manifest_no_match)}) {pattern}"
            ] = manifest_no_match

        if manifest_no_vcf:
            errors[
                f"Samples in manifest with no VCF found "
                f"({len(manifest_no_vcf)})"
            ] = manifest_no_vcf


        # check to ensure all vcfs (and mosdepth files for SNVs) are unarchived
        DXManage().check_archival_state(
            files=vcf_files + mosdepth_files + excluded_intervals_bed_file,
            samples=manifest.keys(),
            unarchive=unarchive
        )

        workflow_details = dxpy.describe(workflow_id)

        stage_folders = DXManage().format_output_folders(
            workflow=workflow_details,
            single_output=single_output_dir,
            time_stamp=start
        )

        parent_folder = make_path(
            single_output_dir, workflow_details['name'], start
        )

        if not manifest:
            # empty manifest after filtering against files etc
            error = f"No samples left after filtering to run {mode} reports on"

            raise RuntimeError(error)

        print(f"\n \nLaunching {mode} reports per sample...")
        start = timer()

        launched_jobs = []
        samples_run = 0

        # initialise per sample summary dict from samples in manifest
        sample_summary = {mode: {k: [] for k in manifest.keys()}}

        # launch reports workflow, once per sample -> set of test codes
        for sample, sample_config in manifest.items():

            all_test_lists = sample_config['tests']
            vcf = sample_config['vcf'][0]  # TODO : need to test for >1 VCF?

            # mapping for current sample name -> index suffix to handle
            # edge case of same test code on same run
            sample_name_to_suffix = {}

            for idx, test_list in enumerate(all_test_lists):
                print(
                    f"[{samples_run+1}/{len(manifest)}] Launching {mode} "
                    f"reports workflow {idx+1}/{len(all_test_lists)} for "
                    f"{sample} with test(s): {test_list}"
                )

                input = deepcopy(config['inputs'])

                # add vcf found for sample to input dict, currently just
                # needs providing to VEP for both workflows
                input[vcf_input_field] = {
                    "$dnanexus_link": {
                        "project": vcf['project'],
                        "id": vcf['id']
                    }
                }

                # format required string inputs of panels and indications
                panels = ';'.join(sample_config['panels'][idx])
                indications = ';'.join(sample_config['indications'][idx])
                codes = '&&'.join(test_list)

                # set prefix for naming output report with integer suffix
                name = (
                    f"{vcf['describe']['name'].split('_')[0]}_"
                    f"{'_'.join(test_list)}_{mode}".replace('__', '_')
                )
                suffix = check_report_index(name=name, reports=xlsx_reports)

                if sample_name_to_suffix.get(name):
                    # we have already launched a report for this sample in
                    # this current job => increment from this
                    suffix = sample_name_to_suffix.get(name) + 1

                    print(
                        f"Already launched report for current sample, "
                        f"will now use suffix {suffix}"
                    )

                sample_name_to_suffix[name] = suffix
                name = f"{name}_{suffix}"

                # CNV vs SNV stage IDs annoyingly all slight differ,
                # add required other inputs to where they need to be
                if mode == 'CNV':
                    input['stage-cnv_generate_bed_vep.panel'] = indications
                    input['stage-cnv_generate_bed_vep.output_file_prefix'] = codes
                    input['stage-cnv_generate_bed_excluded.panel'] = indications
                    input['stage-cnv_generate_bed_excluded.output_file_prefix'] = codes
                    input['stage-cnv_generate_workbook.clinical_indication'] = indications
                    input['stage-cnv_generate_workbook.output_prefix'] = name
                    input['stage-cnv_generate_workbook.panel'] = panels

                    # add run level excluded regions file to input
                    input[
                        'stage-cnv_annotate_excluded_regions.excluded_regions'
                    ] = excluded_intervals_bed
                else:
                    # build mosdepth files as a list of dx_links for athena
                    mosdepth_links = [
                        {"$dnanexus_link": {
                            "project": file['project'],
                            "id": file['id']
                        }}
                        for file in sample_config['mosdepth']
                    ]

                    input['stage-rpt_athena.mosdepth_files'] = mosdepth_links
                    input['stage-rpt_generate_bed_athena.panel'] = indications
                    input['stage-rpt_generate_bed_athena.output_file_prefix'] = codes
                    input['stage-rpt_generate_bed_vep.panel'] = indications
                    input['stage-rpt_generate_bed_vep.output_file_prefix'] = codes
                    input['stage-rpt_generate_workbook.clinical_indication'] = indications
                    input['stage-rpt_generate_workbook.panel'] = panels
                    input['stage-rpt_generate_workbook.output_prefix'] = name
                    input['stage-rpt_athena.name'] = name


                # now we can finally run the reports workflow
                job_handle = dxpy.DXWorkflow(
                    dxid=workflow_id
                ).run(
                    workflow_input=input,
                    rerun_stages=['*'],
                    detach=True,
                    name=f"{workflow_details['name']}_{sample}_{codes} ({mode})",
                    folder=parent_folder,
                    stage_folders=stage_folders,
                    depends_on=parent
                )

                launched_jobs.append(job_handle._dxid)
                sample_summary[mode][sample].append(name)

            # finished launching this samples test job(s) => join up
            # multiple outputs for nicer output viewing
            sample_summary[mode][sample] = '\n'.join(
                sample_summary[mode][sample]
            )

            samples_run += 1
            if samples_run == sample_limit:
                print("Sample limit hit, stopping launching further jobs")
                break

        end = timer()
        print(
            f"Successfully launched {len(launched_jobs)} {mode} reports "
            f"workflows in {round(end - start)}s"
        )
        return launched_jobs, errors, sample_summary


    def artemis(
            self,
            single_output_dir,
            app_id,
            dependent_jobs,
            start,
            qc_xlsx,
            capture_bed,
            snv_output=None,
            cnv_output=None
        ) -> str:
        """
        Launch eggd_artemis to generate xlsx file of download links

        Parameters
        ----------
        single_output_dir : str
            path to single output directory
        app_id : str
            app ID of eggd_artemis
        dependent_jobs : list[str]
            list of jobs to wait to complete before starting
        start : str
            start time of running app for naming output folders
        qc_xlsx : dict
            $dnanexus_link mapping of file input of QC status
        capture_bed : dict
            $dnanexus_link mapping dict of capture bed
        snv_output : str (optional)
            output path of SNV reports (if run), by default None
        cnv_output : str (optional)
            output path of CNV reports (if run), by default None

        Returns
        -------
        str
            job ID of launched job
        """
        details = dxpy.DXApp(app_id).describe()
        path = make_path(single_output_dir, details['name'], start)

        app_input = {
            "snv_path": snv_output,
            "cnv_path": cnv_output,
            "qc_status": qc_xlsx,
            "bed_file": capture_bed
        }

        job = dxpy.DXApp(dxid=app_id).run(
            app_input=app_input,
            project=os.environ.get('DX_PROJECT_CONTEXT_ID'),
            folder=path,
            depends_on=dependent_jobs,
            detach=True
        )

        return job._dxid


    @staticmethod
    def terminate(jobs) -> None:
        """
        Terminate all launched jobs in testing mode

        Parameters
        ----------
        jobs : list
            list of job / analysis IDs
        """
        def terminate_one(job) -> None:
            """dx call to terminate single job"""
            if job.startswith('job'):
                dxpy.DXJob(dxid=job).terminate()
            else:
                dxpy.DXAnalysis(dxid=job).terminate()

        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
            concurrent_jobs = {
                executor.submit(terminate_one, id):
                id for id in sorted(jobs, reverse=True)
            }
            for future in concurrent.futures.as_completed(concurrent_jobs):
                # access returned output as each is returned in any order
                try:
                    future.result()
                except Exception as exc:
                    # catch any errors that might get raised
                    print(
                        "Error terminating job "
                        f"{concurrent_jobs[future]}: {exc}"
                    )

        print("Terminated jobs.")
