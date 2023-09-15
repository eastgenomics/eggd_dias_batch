"""
Functions related to querying and managing objects in DNAnexus, as well
as running jobs.
"""
import json
import os
from pathlib import Path
from pprint import pprint
import re

import dxpy
import pandas as pd

import utils.utils as utils

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
            
            print(f"Assay config file contents:")
            pprint(config)
            return config

        # searching dir for configs, check for valid project:path structure
        assert re.match(r'project-[\d\w]*:/.*', path), (
            f'path to assay configs appears invalid: {path}'
        )

        log.info(f"\nSearching following path for assay configs: {path}")

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
        log.info(f"\nAssay config files found:\n\t{files_ids}")

        highest_config = {}

        for file in files:
            if not file['describe']['archivalState'] == 'live':
                log.info(
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
                highest_config = config_data
        
        log.info(
            f"Highest version config found for {assay} was "
            f"{highest_config.get('version')} from {highest_config.get('dxid')}"
        )

        print(f"Assay config file contents:")
        pprint(highest_config)

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


    def find_files(self, path, dir=None, pattern=None) -> list:
        """
        Search given path in DNAnexus, optionally in a sub directory
        and with a pattern

        Parameters
        ----------
        path : str
            path to where to search
        dir : str (optional)
            sub directory to search, will partially match as /path/dir.*
        pattern : str (optional)
            regex file pattern to search for

        Returns
        -------
        list
            list of files found
        """
        path = path.rstrip('/')  # I define these and not the user but just
        dir = dir.strip('/')     # incase I forget anywhere and have extra /

        print(f"Searching for files in {path}/{dir} with pattern {pattern}")
        files = list(dxpy.find_data_objects(
            name=pattern,
            name_mode='regexp',
            folder=path,
            describe=True
        ))

        if dir:
            # filter down to just those in the given sub dir
            files = [
                x for x in files
                if x['describe']['folder'].startswith(f"{path}/{dir}")
            ]
        
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
        bam_dir = utils.make_path(
            single_output_dir, cnv_config['inputs']['bambais']['folder']
        )

        files = list(dxpy.find_data_objects(
            name=cnv_config['inputs']['bambais']['name'],
            name_mode='regexp',
            project=os.environ.get("DX_PROJECT_CONTEXT_ID"),
            folder=bam_dir,
            describe=True
        ))

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
        folder = utils.make_path(
            single_output_dir,
            f"{app_details['name']}-{app_details['version']}",
            utils.time_stamp()
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
            call_job_id,
            single_output_dir,
            manifest,
            manifest_source,
            config
        ) -> list:
        """
        Run Dias reports workflow on output of CNV calling.

        Matches output files of CNV calling against manifest samples,
        parses config for the workflow and launches workflow per sample.

        Parameters
        ----------
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

        Returns
        -------
        list
            list of job IDs launched
        """
        job_details = dxpy.bindings.dxjob.DXJob(dxid=call_job_id).describe()
        job_output = dxpy.find_data_objects(
            name="segments.vcf$",
            name_mode='regexp',
            folder=job_details.get('folder'),
            describe=True
        )

        # patterns of sample ID and sample file prefix to match on
        if manifest_source == 'Epic':
            pattern = r'^[\d\w]+-[\d\w]+'
        else:
            pattern = r'X[\d]'

        manifest = utils.filter_manifest_samples_by_files(
            manifest=manifest,
            files=job_output,
            pattern=pattern
        )
        

    @staticmethod
    def terminate(jobs) -> None:
        """
        Terminate all launched jobs in testing mode

        Parameters
        ----------
        jobs : list
            list of job / analysis IDs
        """
        for job in jobs:
            if job.startswith('job'):
                dxpy.bindings.dxjob.DXJob(dxid=job).terminate()
            else:
                dxpy.bindings.dxanalysis.DXAnalysis(dxid=job).terminate()
