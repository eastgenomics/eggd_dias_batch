"""
Functions related to querying and managing objects in DNAnexus, as well
as running jobs.
"""
from collections import defaultdict
from copy import deepcopy
import concurrent.futures
import json
import os
import re
from timeit import default_timer as timer

import dxpy
import pandas as pd

from .utils import filter_manifest_samples_by_files, make_path, \
    prettier_print, time_stamp, check_report_index

# for prettier viewing in the logs
pd.set_option('display.max_rows', 100)
pd.set_option('max_colwidth', 1500)


class DXManage():
    """
    Methods for generic handling of dx related things
    """

    def get_assay_config(self, path, file, assay) -> dict:
        """
        Get highest config file from given path for given assay

        Parameters
        ----------
        path : str
            DNAnexus project:path to dir containing assay configs
        file: str
            DNAnexus file ID of config file to use instead of searching
        assay : str
            assay string to return configs for (i.e. CEN or WES)

        Returns
        -------
        dict
            contents of config file
        """
        if file:
            # specified file to use => read in and return
            print("Reading in specified assay config file")
            file = file.get('$dnanexus_link')

            if not file.startswith('project'):
                # just file-xxx provided => find a project the file is in
                file_details = self.get_file_project_context(file)
            else:
                project, file_id = file.split(':')
                file_details = dxpy.bindings.dxfile.DXFile(
                    project=project, dxid=file_id).describe()
            print(
                f"Using assay config file: {file_details['describe']['name']} "
                f"({file_details['project']}:{file_details['id']})"
            )

            config = json.loads(dxpy.bindings.dxfile.DXFile(
                project=file_details['project'], dxid=file_details['id']).read())

            config['name'] = file_details['describe']['name']
            config['dxid'] = file_details['id']

            print("Assay config file contents:")
            prettier_print(config)
            return config

        # searching dir for configs, check for valid project:path structure
        assert re.match(r'project-[\d\w]*:/.*', path), (
            f'path to assay configs appears invalid: {path}'
        )

        print(f"\nSearching following path for assay configs: {path}")

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
                    "Config file not in live state - will not be used:"
                    f"{file['describe']['name']} ({file['id']}"
                )
                continue

            config_data = json.loads(
                dxpy.bindings.dxfile.DXFile(
                    project=file['project'], dxid=file['id']).read())

            if not config_data.get('assay') == assay:
                continue

            if config_data.get('version') > highest_config.get('version'):
                config_data['dxid'] = file['id']
                config_data['name'] = file['describe']['name']
                highest_config = config_data

        print(
            f"Highest version config found for {assay} was "
            f"{highest_config.get('version')} from {highest_config.get('dxid')}"
        )

        print("Assay config file contents:")
        prettier_print(highest_config)

        return highest_config


    def get_file_project_context(self, file) -> str:
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


    def find_files(self, path, subdir='', pattern=None) -> list:
        """
        Search given path in DNAnexus, optionally in a sub directory
        and with a pattern

        Parameters
        ----------
        path : str
            path to where to search
        subdir : str (optional)
            sub directory to search, will partially match as /path/dir.*
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
            describe=True
        ))

        if subdir:
            # filter down to just those in the given sub dir
            files = [
                x for x in files
                if x['describe']['folder'].startswith(f"{path}/{subdir}")
            ]

        not_live = [
            f"{x['describe']['name']} ({x['id']})" for x in files
            if x['describe']['archivalState'] != 'live'
        ]
        if not_live:
            raise RuntimeError(
                f"WARNING: one or more files found in {path} are not in "
                f"a live state: {prettier_print(not_live)}"
            )

        print(f"Found {len(files)} files in {path}/{subdir}")

        return files


    def read_dxfile(self, file) -> list:
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
            contents of file
        
        Raises
        ------
        RuntimeError
            Raised if 'file' argument not in an expected format
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
            project = file_details['project']
            file_id = file_details['id']
            file_name = file_details['describe']['name']
        elif re.match(r'^project-[\d\w]+:file-[\d\w]+', file):
            # nicely provided as project-xxx:file-xxx
            project, file_id = file.split(':')
            file_details = dxpy.bindings.dxfile.DXFile(
                project=project, dxid=file_id).describe()
            file_name = file_details['name']
        else:
            # who knows what's happened, not for me to deal with
            raise RuntimeError(
                f"DXFile not in an expected format: {file}"
            )

        return dxpy.bindings.dxfile.DXFile(
            project=project, dxid=file_id).read().split('\n')


    def format_output_folders(self, workflow, single_output, time_stamp) -> dict:
        """
        Generate dict of output folders for each stage of given workflow
        for passing to dxpy.bindings.dxworkflow.DXWorkflow().run()

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
        print("Generating output folder structure")
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
    Methods for handling exeuction of apps / worklfows
    """
    def cnv_calling(self, config, single_output_dir, exclude, wait) -> str:
        """
        Run CNV calling for given samples in manifest

        Parameters
        ----------
        config : dict
            dict of assay config file
        single_output_dir : str
            path to single output directory
        exclude : list
            list of sample IDs to exclude bam files from calling
        wait : bool
            if to set hold_on_wait to wait on job to finish

        Returns
        -------
        str
            job ID of launch cnv calling job
        """
        print("Building inputs for CNV calling")
        cnv_config = config['modes']['cnv_call']

        # find BAM files and format as $dnanexus_link inputs to add to config
        bam_dir = make_path(
            single_output_dir, cnv_config['inputs']['bambais']['folder']
        )

        files = DXManage().find_files(
            pattern=cnv_config['inputs']['bambais']['name'],
            path=f"{os.environ.get('DX_PROJECT_CONTEXT_ID')}:{bam_dir}"
        )

        print(f"Found {len(files)} .bam/.bai files in {bam_dir}")

        if exclude:
            samples = '\n\t'.join(exclude)
            print(f"Samples specified to exclude from CNV calling:\n\t{samples}")

            # filtering out sample files specified from -iexclude, assuming
            # here there are no typos, sample names are given as found in
            # samplesheet and that bam files are named as sampleID_other_stuff.bam
            exclude_not_present = [
                name for name in exclude
                if name not in [x['describe']['name'] for x in files]
            ]
            if exclude_not_present:
                print(
                    "WARNING: sample ID(s) provided to exclude not present in "
                    f"bam files found for CNV calling:\n\t{exclude_not_present}"
                    "\nIgnoring these and continuing..."
                )

            files = [
                file for file in files
                if not file['describe']['name'].split('_')[0] in exclude
            ]
            print(f"{len(files)} .bam/.bai files after exlcuding")

        files = [{"$dnanexus_link": file} for file in files]
        cnv_config['inputs']['bambais'] = files

        # set output folder relative to single dir
        app_details = dxpy.describe(config.get('cnv_call_app_id'))
        folder = make_path(
            single_output_dir,
            f"{app_details['name']}-{app_details['version']}",
            time_stamp()
        )

        print(f"Running CNV calling, outputting to {folder}")

        job = dxpy.bindings.dxapp.DXApp(dxid=config.get('cnv_call_app_id')).run(
            app_input=cnv_config['inputs'],
            project=os.environ.get('DX_PROJECT_CONTEXT_ID'),
            folder=folder,
            priority='high',
            detach=True,
            instance_type=cnv_config.get('instance_type')
        )

        job_id = job.describe().get('id')
        job_handle = dxpy.bindings.dxjob.DXJob(dxid=job_id)

        if wait:
            print("Holding app until CNV calling completes...")
            try:
                # holds app until job returns success
                job_handle.wait_on_done()
            except dxpy.exceptions.DXJobFailureError as err:
                # dx job error raised (i.e. failed, timed out, terminated)
                raise RuntimeError(
                    f"CNV calling failed in job {job_id}:\n\n{err}"
                )
            print("CNV calling completed successfully\n")
        else:
            print(f'CNV calling launched: {job_id}\n')

        return job_id


    def cnv_reports(
            self,
            workflow_id,
            call_job_id,
            single_output_dir,
            manifest,
            manifest_source,
            config,
            start,
            sample_limit,
            parent
        ) -> list:
        """
        Run Dias reports workflow on output of CNV calling.

        Matches output files of CNV calling against manifest samples,
        parses config for the workflow and launches workflow per sample.

        Parameters
        ----------
        workflow_id : str
            dxid of CNV reports workflow
        call_job_id : str
            job ID of CNV calling to use output from
        single_output_dir : str
            dnanexus path to Dias single output
        manifest : dict
            mapping of sampleID -> testCodes parsed from manifest
        manifest_source : str
            source of manifest (Epic or Gemini), required for filtering
            pattern against sample name 
        config : dict
            config for assay, defining fixed inputs for workflow
        start : str
            start time of running app for naming output folders
        sample_limit : int
            no. of samples to launch jobs for
        parent : list | None
            single item list of parent dias batch job ID to use when
            testing to stop jobs running

        Returns
        -------
        list
            list of job IDs launched
        dict
            dict of any errors found (i.e. samples missing files)
        
        Raises
        ------
        RuntimeError
            Raised when an excluded_intervals.bed file can't be found
        """
        print("Configuring inputs for CNV reports")

        # get required files
        job_details = dxpy.bindings.dxjob.DXJob(dxid=call_job_id).describe()

        segment_vcfs = list(DXManage().find_files(
            path=f"{job_details.get('project')}:{job_details.get('folder')}",
            pattern="segments.vcf$"
        ))
        excluded_intervals_bed = list(DXManage().find_files(
            path=f"{job_details.get('project')}:{job_details.get('folder')}",
            pattern="_excluded_intervals.bed$"
        ))

        # find all previous xlsx reports to use for indexing report names
        xlsx_reports = DXManage().find_files(
            path=single_output_dir,
            pattern=r".xlsx$"
        )
        xlsx_reports = [
            x['describe']['name'] for x in xlsx_reports
        ]

        if not excluded_intervals_bed:
            raise RuntimeError(
                f"Failed to find exlcuded intervals bed file from {call_job_id}"
            )
        else:
            excluded_intervals_bed = {
                "$dnanexus_link": {
                    "project": excluded_intervals_bed[0]['project'],
                    "id": excluded_intervals_bed[0]['id']
                }
            }

        print(
            f"Found {len(segment_vcfs)} segments.vcf files from "
            f"{job_details.get('folder')} and {len(xlsx_reports)} "
            f"previous xlsx reports"
        )

        # patterns of sample ID and sample file prefix to match on
        if manifest_source == 'Epic':
            pattern = r'^[\d\w]+-[\d\w]+'
        else:
            pattern = r'X[\d]+'

        # ensure we have a vcf per sample, exclude those that don't have one
        manifest, manifest_no_match, manifest_no_vcf = filter_manifest_samples_by_files(
            manifest=manifest,
            files=segment_vcfs,
            name='segment_vcf',
            pattern=pattern
        )

        # gather errors to display later
        errors = {}

        if manifest_no_match:
            errors[
                f"Samples in manifest not matching pattern ({len(manifest_no_match)}) {pattern}:"
            ] = manifest_no_match

        if manifest_no_vcf:
            errors[
                f"Samples in manifest with no VCF found ({len(manifest_no_vcf)}): "
            ] = manifest_no_vcf

        workflow_details = dxpy.describe(workflow_id)

        stage_folders = DXManage().format_output_folders(
            workflow=workflow_details,
            single_output=single_output_dir,
            time_stamp=start
        )

        print("Launching CNV reports per sample...")
        start = timer()

        launched_jobs = []
        sample_summary = {'CNV': {}}
        samples_run = 0
        # launch reports workflow, once per sample - set of test codes
        for sample, sample_config in manifest.items():

            all_test_lists = sample_config['tests']
            segment_vcf = sample_config['segment_vcf'][0]

            # mapping for current sample name -> index suffix to handle
            # edge case of same test code on same run
            sample_name_to_suffix = {}

            for idx, test_list in enumerate(all_test_lists):
                print(
                    f"[{samples_run+1}/{len(manifest)}] Launching CNV "
                    f"reports workflow {idx+1}/{len(all_test_lists)} for "
                    f"{sample} with test(s): {test_list}"
                )

                input = deepcopy(config['inputs'])
                input['stage-cnv_vep.vcf'] = {
                    "$dnanexus_link": {
                        "project": segment_vcf['project'],
                        "id": segment_vcf['id']
                    }
                }

                # add run level excluded regions file to input
                input[
                    'stage-cnv_annotate_excluded_regions.excluded_regions'
                ] = excluded_intervals_bed

                # add required string inputs of panels and indications
                panels = ';'.join(sample_config['panels'][idx])
                indications = ';'.join(sample_config['indications'][idx])
                codes = '&&'.join(test_list)

                input['stage-cnv_generate_bed_vep.panel'] = indications
                input['stage-cnv_generate_bed_vep.output_file_prefix'] = codes
                input['stage-cnv_generate_bed_excluded.panel'] = indications
                input['stage-cnv_generate_bed_excluded.output_file_prefix'] = codes
                input['stage-cnv_generate_workbook.clinical_indication'] = indications
                input['stage-cnv_generate_workbook.panel'] = panels

                # set prefix for naming output report with integer suffix
                name = (
                    f"{segment_vcf['describe']['name'].split('_')[0]}_"
                    f"{'_'.join(test_list).replace('__', '_')}_CNV"
                )
                suffix = check_report_index(name=name, reports=xlsx_reports)

                if sample_name_to_suffix.get(name):
                    # we have already launched a report for this sample in
                    # this current job => increment from this
                    suffix = sample_name_to_suffix.get(name) + 1

                    print(
                        f"Already launched report for current sample, "
                        f"will now use suffix _{suffix}"
                    )

                sample_name_to_suffix[name] = suffix
                name = f"{name}_{suffix}"

                input['stage-cnv_generate_workbook.output_prefix'] = name

                job_handle = dxpy.bindings.dxworkflow.DXWorkflow(
                    dxid=workflow_id
                ).run(
                    workflow_input=input,
                    rerun_stages=['*'],
                    detach=True,
                    name=f"{workflow_details['name']}_{sample}_{codes} (CNV)",
                    stage_folders=stage_folders,
                    depends_on=parent
                )

                launched_jobs.append(job_handle._dxid)
                if not sample_summary['CNV'].get(sample):
                    sample_summary['CNV'][sample] = [name]
                else:
                    sample_summary['CNV'][sample].append(name)

            # join up multiple outputs for nicer output viewing
            sample_summary['CNV'][sample] = '\n'.join(
                sample_summary['CNV'][sample]
            )

            samples_run += 1
            if samples_run == sample_limit:
                print("Sample limit hit, stopping launching further jobs")
                break

        end = timer()
        print(
            f"Successfully launched {len(launched_jobs)} CNV reports "
            f"workflows in {round(end - start)}s"
        )

        return launched_jobs, errors, sample_summary


    def snv_reports(
        self,
        workflow_id,
        single_output_dir,
        manifest,
        manifest_source,
        config,
        mode,
        start,
        sample_limit,
        parent
        ) -> list:
        """
        Run Dias reports workflow for either SNV or mosaic reports

        Parameters
        ----------
        workflow_id : str
            dxid of Dias reports workflow
        single_output_dir : str
            dnanexus path to Dias single output
        manifest : dict
            mapping of sampleID -> testCodes parsed from manifest
        manifest_source : str
            source of manifest (Epic or Gemini), required for filtering
            pattern against sample name
        config : dict
            config for assay, defining fixed inputs for workflow
        mode : str
            controls if running reports on mosaic (mutect2) output or
            for SNVs
        start : str
            start time of running app for naming output folders
        sample_limit : int
            no. of samples to launch jobs for
        parent : list | None
            single item list of parent dias batch job ID to use when
            testing to stop jobs running

        Returns
        -------
        list
            list of job IDs launched
        dict
            dict of any errors found (i.e samples with no files)
        """
        print(f"Configuring inputs for {mode} reports")
        # find .vcf or .vcf.gz but NOT .g.vcf
        vcf_files = DXManage().find_files(
            path=single_output_dir,
            subdir=config.get('vcf_subdir'),
            pattern=r"^[^\.]*(?!\.g)\.vcf(\.gz)?$"
        )

        mosdepth_files = DXManage().find_files(
            path=single_output_dir,
            subdir='eggd_mosdepth',
            pattern=r"per-base.bed.gz$|reference.txt$"
        )

        # find all previous xlsx reports to use for indexing report names
        xlsx_reports = DXManage().find_files(
            path=single_output_dir,
            pattern=r".xlsx$"
        )
        xlsx_reports = [
            x['describe']['name'] for x in xlsx_reports
        ]

        print(
            f"Found {len(vcf_files)} vcf files from "
            f"{single_output_dir} in subdir {config.get('vcf_subdir')}, "
            f"{len(mosdepth_files)} from {single_output_dir} in subdir "
            f"'mosdepth' and {len(xlsx_reports)} previous xlsx reports"
        )

        if not vcf_files or not mosdepth_files:
            raise RuntimeError(
                "Found no vcf_files and / or mosdepth files!"
            )

        # patterns of sample ID and sample file prefix to match on
        if manifest_source == 'Epic':
            pattern = r'^[\d\w]+-[\d\w]+'
        else:
            pattern = r'X[\d]+'

        # ensure we have a vcf and mosdepth files per sample,
        # exclude those that don't have one
        manifest, manifest_no_match, manifest_no_vcf = \
            filter_manifest_samples_by_files(
                manifest=manifest,
                files=vcf_files,
                name='vcf',
                pattern=pattern
            )

        manifest, _, manifest_no_mosdepth = filter_manifest_samples_by_files(
            manifest=manifest,
            files=mosdepth_files,
            name='mosdepth',
            pattern=pattern
        )

        # gather errors to display in summary report
        errors = {}

        if manifest_no_match:
            errors[
                f"Samples in manifest not matching pattern ({len(manifest_no_match)}) {pattern}:"
            ] = manifest_no_match

        if manifest_no_vcf:
            errors[
                f"Samples in manifest with no VCF found ({len(manifest_no_vcf)}):"
            ] = manifest_no_vcf

        if manifest_no_mosdepth:
            errors[
                f"Samples in manifest with no mosdepth files found ({len(manifest_no_mosdepth)}):"
            ] = manifest_no_mosdepth

        workflow_details = dxpy.describe(workflow_id)

        stage_folders = DXManage().format_output_folders(
            workflow=workflow_details,
            single_output=single_output_dir,
            time_stamp=start
        )

        print(f"Launching {mode} reports per sample...")
        start = timer()

        launched_jobs = []
        sample_summary = {mode: {}}
        samples_run = 0

        # launch reports workflow, once per sample - set of test codes
        for sample, sample_config in manifest.items():

            all_test_lists = sample_config['tests']
            vcf = sample_config['vcf'][0]

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
                input['stage-rpt_vep.vcf'] = {
                    "$dnanexus_link": {
                        "project": vcf['project'],
                        "id": vcf['id']
                    }
                }

                # build mosdepth files as a list of dx_links for athena
                mosdepth_links = [
                    {"$dnanexus_link": {
                        "project": file['project'],
                        "id": file['id']
                    }}
                    for file in sample_config['mosdepth']
                ]
                input['stage-rpt_athena.mosdepth_files'] = mosdepth_links

                # add required string inputs of panels and indications
                panels = ';'.join(sample_config['panels'][idx])
                indications = ';'.join(sample_config['indications'][idx])
                codes = '&&'.join(test_list)

                input['stage-rpt_generate_bed_athena.panel'] = indications
                input['stage-rpt_generate_bed_athena.output_file_prefix'] = codes
                input['stage-rpt_generate_bed_vep.panel'] = indications
                input['stage-rpt_generate_bed_vep.output_file_prefix'] = codes
                input['stage-rpt_generate_workbook.clinical_indication'] = indications
                input['stage-rpt_generate_workbook.panel'] = panels

                # set prefix for naming output report with integer suffix
                name = (
                    f"{vcf['describe']['name'].split('_')[0]}_"
                    f"{'_'.join(test_list).replace('__', '_')}_{mode}"
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

                input['stage-rpt_generate_workbook.output_prefix'] = name
                input['stage-rpt_athena.name'] = name

                job_handle = dxpy.bindings.dxworkflow.DXWorkflow(
                    dxid=workflow_id
                ).run(
                    workflow_input=input,
                    rerun_stages=['*'],
                    detach=True,
                    name=f"{workflow_details['name']}_{sample}_{codes} ({mode})",
                    stage_folders=stage_folders,
                    depends_on=parent
                )

                launched_jobs.append(job_handle._dxid)

                if not sample_summary[mode].get(sample):
                    sample_summary[mode][sample] = [name]
                else:
                    sample_summary[mode][sample].append(name)

            # join up multiple outputs for nicer output viewing
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
                dxpy.bindings.dxjob.DXJob(dxid=job).terminate()
            else:
                dxpy.bindings.dxanalysis.DXAnalysis(dxid=job).terminate()

        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
            concurrent_jobs = {
                executor.submit(terminate_one, id): id for id in jobs
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
