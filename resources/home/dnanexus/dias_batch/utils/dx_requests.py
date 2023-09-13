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

from .utils import make_path, time_stamp


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


    def read_dxfile_to_dataframe(self, file, names, types=None) -> pd.DataFrame:
        """
        Read delimited DXFile object into pd.DataFrame

        Abstracted method for reading in files such as manifest and genepanels

        Parameters
        ----------
        file : str | dict
            file ID of DXFile to read, may be passed as 'file-xxx',
            'project-xxx:file-xxx' or {'$dnanexus_link': '[project-xxx:]file-xxx'}
        names : list
            list of names to use for columns
        types : str | dict
            either single type as a string or mapping of column: type

        Returns
        -------
        pd.DataFrame
            dataframe of files contents
        
        Raises
        ------
        RuntimeError
            Raised if 'file' argument not in an expected format
        """
        if not file:
            # None passed, not sure if I need to handle this but keep tripping
            # myself up with tests so just going to return and probably
            # end up moving the error along somewhere else but oh well
            print("Empty file passed to read_dxfile_todataframe() :sadpepe:")
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

        contents = dxpy.bindings.dxfile.DXFile(
            project=project, dxid=file_id).read()
        contents = [row.split('\t') for row in contents.split('\n') if row]

        df = pd.DataFrame(contents, columns=names)
        if types:
            df = df.astype(types)

        print(
            f"File read in to dataframe: {file_name} "
            f"({project}:{file_id})\n\n{df}"
        )

        return df


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

        files = list(dxpy.find_data_objects(
            name=cnv_config['inputs']['bambais']['name'],
            name_mode='regexp',
            project=os.environ.get("DX_PROJECT_CONTEXT_ID"),
            folder=bam_dir,
            describe=True
        ))

        print(f"Found {len(files)} .bam/.bai files in {bam_dir}")

        if exclude:
            samples = '\n\t'.join(exlcude.split(','))
            print(f"Samples specified to exclude from CNV calling:\n\t{samples}")

            # filtering out sample files specified from -iexclude, assuming
            # here there are no typos, sample names are given as found in
            # samplesheet and that bam files are named as sampleID_other_stuff.bam
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
