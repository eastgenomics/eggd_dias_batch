"""
Functions related to querying and managing objects in DNAnexus, as well
as running jobs.
"""
import json
import os
from pathlib import Path
import re

import dxpy
import pandas as pd

from .utils import time_stamp


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
            if not file.startswith('project'):
                file = self.get_file_project_context(file)

            return json.loads(dx.bindings.dxfile.DXFile(
                project=file['project'], dxid=file['id']).read())

        # searching dir for configs, check for valid project:path structure
        assert re.match(r'project-[\d\w]*:/.*', path), (
            f'path to assay configs appears invalid: {path}'
        )

        log.info(f"\nSearching following path for assay configs: {path}")

        project, project_path = path.split(':')

        files = list(dx.find_data_objects(
            name="*.json",
            name_mode='glob',
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
                dx.bindings.dxfile.DXFile(
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
        str
            project:file format of file
        """
        files = list(dx.find_data_objects(
            name=file,
            project=project,
            folder=project_path,
            describe=True
        ))

        files = [x for x in files if x['describe']['archivalState'] == 'live']
        assert files, f"No live files could be found for the ID: {file}"

        return f"{files[0]['project']}:{file}"


    def read_manifest(self, file) -> pd.DataFrame:
        """
        Read in manifest file to dataframe

        Parameters
        ----------
        file : str
            file ID of manifest file to use
        
        Returns
        -------
        pd.DataFrame
            dataframe of manifest file
        """
        if not file.startswith('project'):
            file = self.get_file_project_context(file)

        contents = dx.bindings.dxfile.DXFile(
            project=file['project'], dxid=file['id']).read()
        
        return pd.DataFrame(contents)


class DXExecute():
    """
    Methods for handling exeuction of apps / worklfows
    """
    def run_cnv_calling(self, config, single_output_dir, exclude, wait):
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
        cnv_config = config.get('inputs').get('cnv_call')

        # find BAM files and format as $dnanexus_link inputs to add to config
        bam_dir = Path.joinpath(
            cnv_config['inputs']['bambais']['folder'], single_output_dir)

        files = list(dx.find_data_objects(
            name=cnv_config['inputs']['bambais']['name'],
            name_mode='glob',
            project=os.environ.get("DX_PROJECT_CONTEXT_ID"),
            folder=bam_dir,
            describe=True
        ))
        files = [file for file in files if not file['describe']['name'] in exclude]
        files = [{"$dnanexus_link": file} for file in files]
        cnv_config['bambais'] = files

        app_details = dxpy.describe(config.get('cnv_call_app_id'))
        folder = path.joinpath(
            single_output_dir,
            (
                f"{app_details['describe']['name']}-"
                f"{app_details['details']['version']}-"
                f"{time_stamp}"
            )
        )

        job = dx.bindings.dxapp.DXApp(dxid=config.get('cnv_call_app_id')).run(
            app_input=cnv_config,
            folder=folder,
            priority='high',
            hold_on_wait=wait,
            instance_type=cnv_config.get('instance_type')
        )

        return job


