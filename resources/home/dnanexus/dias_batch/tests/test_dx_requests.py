"""
The majority of functions in dx_requests.py relate to interacting with
DNAnexus via dxpy API calls to either manage data (in DXManage) or for
launching jobs (in DXExecute).

Functions not covered by unit tests:
    - DXManage.read_assay_config() - mostly just calls DXFile.read() on
        provided dx file ID
    - DXManage().read_dxfile() - reads file object from given dx
        file ID, not expected to raise any errors
    - Everything in DXExecute - all functions relate to launching jobs,
        going to test these manually by running the app (probably, we shall
        see if I get the motivation to try patch things well to test them)
"""
import sys
import unittest
from unittest.mock import patch

import dxpy
import pandas as pd
import pytest


sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))

from utils.dx_requests import DXManage


TEST_DATA_DIR = (
    os.path.join(os.path.dirname(__file__), 'test_data')
)


class TestDXManageGetAssayConfig(unittest.TestCase):
    """
    Tests for DXManage.get_assay_config()

    Function either takes a path and assay string to search in DNAnexus
    and return the highest config file version for
    """
    def test_error_raised_when_path_invalid(self):
        """
        AssertionError should be raised if path param is not valid
        """
        expected_error = 'path to assay configs appears invalid: invalid_path'

        with pytest.raises(AssertionError, match=expected_error):
            DXManage().get_assay_config(path='invalid_path', assay='')


    @patch('utils.dx_requests.dxpy.find_data_objects')
    def test_error_raised_when_no_config_files_found(self, test_patch):
        """
        AssertionError should be raised if no JSON files found, patch
        the return of the dxpy.find_data_objects() call to be empty and
        check we raise an error correctly
        """
        test_patch.return_value = []

        expected_error = (
            'No config files found in given path: project-xxx:/test_path'
        )
        with pytest.raises(AssertionError, match=expected_error):
            DXManage().get_assay_config(path='project-xxx:/test_path', assay='')


    @patch('utils.dx_requests.json.loads')
    @patch('utils.dx_requests.dxpy.bindings.dxfile.DXFile')
    @patch('utils.dx_requests.dxpy.find_data_objects')
    def test_error_raised_when_no_config_file_found_for_assay(
            self,
            mock_find,
            mock_file,
            mock_read
        ):
        """
        AssertionError should be raised if we find some JSON files but
        after parsing through none of them match our given assay string
        against the 'assay' key in the config
        """
        # set output of find to be minimal describe call output with
        # required keys for iterating over
        mock_find.return_value = [
            {
                'project': 'project-xxx',
                'id': 'file-xxx',
                'describe' : {
                    'name': 'config1.json',
                    'archivalState': 'live'
                }
            }
        ]

        # patch the DXFile object that read() gets called on
        mock_file.return_value = dxpy.bindings.dxfile.DXFile

        # patch the output from DXFile.read() to just be all the same dict
        mock_read.return_value = {'assay': 'CEN', 'version': '1.0.0'}

        expected_error = (
            "No config file was found for test from project-xxx:/test_path"
        )

        with pytest.raises(AssertionError, match=expected_error):
            DXManage().get_assay_config(
                path='project-xxx:/test_path',
                assay='test'
            )


    @patch('utils.dx_requests.json.loads')
    @patch('utils.dx_requests.dxpy.bindings.dxfile.DXFile')
    @patch('utils.dx_requests.dxpy.find_data_objects')
    def test_highest_version_correctly_selected(
            self,
            mock_find,
            mock_file,
            mock_read
        ):
        """
        Test that when multiple configs are found for an assay, the
        highest version is correctly returned. We're using
        packaging.version.Version to compare versions parsed from
        the config files so this _should_ work as we expect
        """
        # set output of find to be minimal describe call output with
        # required keys for iterating over, here we need a dict per
        # `mock_read` return values that we want to test with
        mock_find.return_value = [
            {
                'project': 'project-xxx',
                'id': 'file-xxx',
                'describe' : {
                    'name': 'config.json',
                    'archivalState': 'live'
                }
            }
        ] * 5

        # patch the DXFile object that read() gets called on
        mock_file.return_value = dxpy.bindings.dxfile.DXFile

        # patch the output from DXFile.read() to simulate looping over
        # the return of reading multiple configs
        mock_read.side_effect = [
            {'assay': 'test', 'version': '1.0.0'},
            {'assay': 'test', 'version': '1.1.0'},
            {'assay': 'test', 'version': '1.0.10'},
            {'assay': 'test', 'version': '1.1.11'},
            {'assay': 'test', 'version': '1.2.1'}
        ]

        config = DXManage().get_assay_config(
            path='project-xxx:/test_path',
            assay='test'
        )

        assert config['version'] == '1.2.1', (
            "Incorrect config file version returned"            
        )


class TestDXManageGetFileProjectContext(unittest.TestCase):
    """
    Tests for DXManage.get_file_project_context()

    Function takes a DXFile ID and returns a project ID in which
    the file has been found in a live state
    """

    @patch('utils.dx_requests.dxpy.DXFile.describe')
    @patch('utils.dx_requests.dxpy.DXFile')
    @patch('utils.dx_requests.dxpy.find_data_objects')
    def test_no_live_files(
            self,
            mock_find,
            mock_file,
            mock_describe
        ):
        """
        Test that when no files in a live state are found that an
        AssertionError is raised
        """
        # patch the DXFile object to nothing as we won't use it,
        # and the output of dx find to be a minimal set of describe calls
        # mock_file.return_value = dxpy.DXFile(dxid='file-xxx')
        mock_describe.return_value = {}
        mock_find.return_value = [
            {
                'project': 'project-xxx',
                'id': 'file-xxx',
                'describe' : {
                    'archivalState': 'archived'
                }
            },
            {
                'project': 'project-xxx',
                'id': 'file-xxx',
                'describe' : {
                    'archivalState': 'archival'
                }
            }
        ]

        correct_error = 'No live files could be found for the ID: file-xxx'

        with pytest.raises(AssertionError, match=correct_error):
            DXManage().get_file_project_context(file='file-xxx')


class TestDXManageFindFiles(unittest.TestCase):
    """
    Tests for DXManage.find_files()

    Function takes a path in DNAnexus and returns a set of files,
    will optionally filter these to a sub directory and also regex
    pattern for the file name to match
    """

    @patch('utils.dx_requests.dxpy.find_data_objects')
    def test_files_just_path_returned(self, mock_find):
        """
        Test when a set of files is returned from dxpy.find_data_objects()
        and no sub dir or pattern is specified that all the files are returned
        """
        mock_find.return_value = [
            {
                'project': 'project-xxx',
                'id': 'file-xxx',
                'describe' : {
                    'name': 'file1',
                    'archivalState': 'live'
                }
            },
            {
                'project': 'project-xxx',
                'id': 'file-xxx',
                'describe' : {
                    'name': 'file2',
                    'archivalState': 'live'
                }
            }
        ]

        files = DXManage().find_files(path='project-xxx:/some_path/')

        assert files == mock_find.return_value, 'Incorrect files returned'


    @patch('utils.dx_requests.dxpy.find_data_objects')
    def test_sub_dir_filters_correctly(self, mock_find):
        """
        Test when a sub dir is provided, the files are correctly filtered
        """
        mock_find.return_value = [
            {
                'project': 'project-xxx',
                'id': 'file-xxx',
                'describe' : {
                    'name': 'file1',
                    'archivalState': 'live',
                    'folder': '/path_to_files/subdir1/app1'
                }
            },
            {
                'project': 'project-xxx',
                'id': 'file-xxx',
                'describe' : {
                    'name': 'file2',
                    'archivalState': 'live',
                    'folder': 'path_to_files/subdir2/app1'
                }
            }
        ]

        files = DXManage().find_files(
            path='project-xxx:/path_to_files/',
            subdir='/subdir1'
        )

        assert files == [mock_find.return_value[0]], (
            'Incorrect file returned when filtering to subdir'
        )


class TestDXManageCheckArchivalState(unittest.TestCase):
    """
    TODO
    """


class TestDXManageUnarchiveFiles(unittest.TestCase):
    """
    TODO
    """


class TestDXManageFormatOutputFolders(unittest.TestCase):
    """
    Tests for DXManage.format_output_folders()

    Function takes all the stages of a workflow, single output directory
    and a time stamp string to build a mapping of stages -> output folders
    """

    @patch('utils.dx_requests.dxpy.describe')
    def test_correct_folder_applet(self, mock_describe):
        """
        Test when an applet is inlcuded as a stage that the path is
        correctly set, applets are treat differently to apps as the
        'executable' key in the workflow details is just the applet ID
        instead of the human name and version for apps
        """
        mock_describe.return_value = {'name': 'applet1-v1.2.3'}

        workflow_details = {
            'name': 'workflow1',
            'stages': [
                {
                    'id': 'stage1',
                    'executable': 'applet-xxx'
                }
            ]
        }

        returned_stage_folder = DXManage().format_output_folders(
            workflow=workflow_details,
            single_output='some_output_path',
            time_stamp='010123_1303'
        )

        correct_stage_folder = {
            "stage1": "/some_output_path/workflow1/010123_1303/applet1-v1.2.3/"
        }

        assert correct_stage_folder == returned_stage_folder, (
            "Incorrect stage folders returned for applet"
        )
    
    def test_correct_folder_app(self):
        """
        Test when an app is included as a stage that the path is correctly
        set from its 'executable' value as this will contain the name
        and the version for the app
        """
        workflow_details = {
            'name': 'workflow1',
            'stages': [
                {
                    'id': 'stage1',
                    'executable': 'app-xxx/1.2.3'
                }
            ]
        }

        correct_stage_folder = {
            "stage1": "/some_output_path/workflow1/010123_1303/xxx-1.2.3/"
        }

        returned_stage_folder = DXManage().format_output_folders(
            workflow=workflow_details,
            single_output='some_output_path',
            time_stamp='010123_1303'
        )

        assert correct_stage_folder == returned_stage_folder, (
            "Inavlid stage folders returned for app"
        )
