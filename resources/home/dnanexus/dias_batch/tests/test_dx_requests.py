"""
The majority of functions in dx_requests.py relate to interacting with
DNAnexus via dxpy API calls to either manage data (in DXManage) or for
launching jobs (in DXExecute).

Functions not covered by unit tests:
    - Everything in DXExecute - all functions relate to launching jobs,
        going to test these manually by running the app (probably, we shall
        see if I get the motivation to try patch things well to test them)
"""
import os
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

class TestReadAssayConfigFile():
    """
    Tests for DXManage.read_assaay_config_file()

    Function is used where a specific assay config file is provided, and
    reads this into a dict object
    """

    @patch('utils.dx_requests.dxpy.DXFile')
    @patch('utils.dx_requests.DXManage.read_dxfile')
    def test_config_correctly_read(self, mock_read, mock_file):
        """
        Test config file is correctly read in, function uses already tested
        DXManage.read_dxfile() to read the contents into a list, so this will
        just test that the contents is returned as a dict and the filename
        is added under the key 'name'
        """
        # minimal describe call return from config file
        mock_file.return_value.describe.return_value = {
            'id': 'file-xxx',
            'name': 'testAssayConfig.json'
        }

        # minimal example of what would be returned from DXManage.read_dxfile
        mock_read.return_value = [
            '{"assay": "test", "version": "1.0.0"}'
        ]

        contents = DXManage().read_assay_config_file(file='file-xxx')

        correct_contents = {
            "assay": "test",
            "version": "1.0.0",
            "dxid": "file-xxx",
            "name": "testAssayConfig.json"
        }

        assert contents == correct_contents, (
            "Contents parsed from config file incorrect"
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


class TestReadDXfile():
    """
    Tests for DXManage.read_dxfile()

    Generic method for reading the contents of a DNAnexus file into a
    list of strings, accepts file ID input as some form of string or
    $dnanexus_link mapping
    """
    def test_none_object_passed(self, capsys):
        """
        If an empty object gets passed we should just print and return
        """
        DXManage().read_dxfile(file=None)
        stdout = capsys.readouterr().out

        assert 'Empty file passed to read_dxfile() :sadpepe:' in stdout, (
            "Function didn't return as expected for empty input"
        )

    @patch('utils.dx_requests.dxpy.DXFile.read')
    @patch('utils.dx_requests.dxpy.DXFile')
    def test_file_as_dict(self, mock_file, mock_read):
        """
        Test when file input is a dict (i.e. $dnanexus_link mapping) that
        we correctly parse the link to query with
        
        set variables for reading the file
        """
        file = {
            "$dnanexus_link": "project-xxx:file-xxx"
        }

        # project and file should get split and pass the assert, we have
        # patched DXFile.read() so nothing will get returned as we expect
        DXManage().read_dxfile(file=file)


    @patch('utils.dx_requests.DXManage.get_file_project_context')
    @patch('utils.dx_requests.dxpy.DXFile.read')
    @patch('utils.dx_requests.dxpy.DXFile')
    def test_file_as_just_file_id(self, mock_file, mock_read, mock_context):
        """
        Test when we provide file ID as just 'file-xxx' that it we call
        DXManage.get_file_project_context to return the project string,
        and then this passes through the function with no errors raised
        """
        # patch a minimal DXObject response
        mock_context.return_value = {
            'project': 'project-xxx',
            'id': 'file-xxx'
        }

        # project and file should get split from the get_file_project_context
        # response and pass the assert, we have patched DXFile.read() so
        # nothing will get returned as we expect
        DXManage().read_dxfile(file='file-xxx')


    @patch('utils.dx_requests.DXManage.get_file_project_context')
    @patch('utils.dx_requests.dxpy.DXFile.read')
    @patch('utils.dx_requests.dxpy.DXFile')
    def test_assertion_error_raised(self, mock_file, mock_read, mock_context):
        """
        Test when we provide file ID as just 'file-xxx' that it we call
        DXManage.get_file_project_context to return the project string,
        and that if there is something wrong in the format of the response
        (i.e. its empty but somehow didn't raise an error), we catch this
        with an AssertionError
        """
        # patch a DXObject response as being empty
        mock_context.return_value = {}

        with pytest.raises(
            AssertionError,
            match=r'Missing project and \/ or file ID - project: None, file: None'
        ):
            DXManage().read_dxfile(file='file-xxx')


    @patch('utils.dx_requests.dxpy.DXFile.read')
    @patch('utils.dx_requests.dxpy.DXFile')
    def test_file_as_project_and_file(self, mock_file, mock_read):
        """
        Test when file input is string with both project and file IDs
        that this get correctly split and used
        
        set variables for reading the file
        """
        # project and file should get split and pass the assert, we have
        # patched DXFile.read() so nothing will get returned as we expect
        DXManage().read_dxfile(file='project-xxx:file-xxx')


    def test_invalid_string_raises_error(self):
        """
        Test if an invalid string is passed that an error is raised
        """
        with pytest.raises(
            RuntimeError,
            match=r'DXFile not in an expected format: invalid_str'
        ):
            DXManage().read_dxfile(file='invalid_str')


class TestDXManageCheckArchivalState():
    """
    Tests for DXManage.check_archival_state()

    Function takes in a list of files (and optionally a list of sample names
    to filter by), and checks the archival state of the files to ensure all
    are live before launching jobs
    """
    # minimal dxpy.find_data_objects() return that we expect to pass in
    files = [
        {
            'id': 'file-xxx',
            'describe': {
                'name': 'sample1-file1',
                'archivalState': 'live'
            }
        },
        {
            'id': 'file-xxx',
            'describe': {
                'name': 'sample2-file1',
                'archivalState': 'live'
            }
        },
        {
            'id': 'file-xxx',
            'describe': {
                'name': 'sample3-file1',
                'archivalState': 'live'
            }
        },
        {
            'id': 'file-xxx',
            'describe': {
                'name': 'sample4-file1',
                'archivalState': 'live'
            }
        },
    ]

    # same as above but with an archived file added in
    files_w_archive = files + [
        {
            'id': 'file-xxx',
            'describe': {
                'name': 'sample5-file1',
                'archivalState': 'archived'
            }
        }
    ]

    def test_all_live(self, capsys):
        """
        Test no error is raised when all provided files are live
        """
        DXManage().check_archival_state(
            files=self.files,
            unarchive=False
        )

        # since we don't explicitly return anything when there are no
        # archived files, check stdout for expected string printed
        # to ensure the function passed through all checks to the end
        stdout = capsys.readouterr().out

        assert 'No required files in archived state' in stdout, (
            'Expected print for all live files not in captured stdout'
        )


    def test_error_raised_for_archived_files(self):
        """
        Test when files contains an archived file that a RuntimeError
        is correctly raised
        """
        with pytest.raises(
            RuntimeError,
            match='Files required for analysis archived'
        ):
            DXManage().check_archival_state(
            files=self.files_w_archive,
            unarchive=False
        )


    def test_archived_files_filtered_out_when_not_in_sample_list(self, capsys):
        """
        Test when a list of sample names is provided that any files for other
        samples are filtered out, we will test this by adding an archived file
        for a non-matching sample and checking it is removed
        """
        # provide list of sample names to filter by
        DXManage().check_archival_state(
            files=self.files_w_archive,
            unarchive=False,
            samples=['sample1', 'sample2', 'sample3', 'sample4']
        )

        # since we don't explicitly return anything for all being live check
        # stdout for expected string printed to ensure we got where we expect
        stdout = capsys.readouterr().out

        assert 'No required files in archived state' in stdout, (
            'Expected print for all live files not in captured stdout'
        )


    @patch('utils.dx_requests.DXManage.unarchive_files')
    def test_unarchive_files_called_when_specified(self, mock_unarchive):
        """
        Test when we have archived files and unarchive=True specified that
        we call the function to start unarchiving
        """
        DXManage().check_archival_state(
            files=self.files_w_archive,
            unarchive=True
        )

        assert mock_unarchive.called, (
            'DXManage.unarchive_files not called for unarchive=True'
        )


class TestDXManageUnarchiveFiles():
    """
    Tests for DXManage.unarchive_files()

    Function called by DXManage.check_archival_state where one or more
    archived files found and unarchive=True set, will go through the
    given file IDs and start the unarchiving process
    """
    # minimal dxpy.find_data_objects() return that we expect to unarchive
    files = [
        {
            'project': 'project-xxx',
            'id': 'file-xxx',
            'describe': {
                'name': 'sample1-file1',
                'archivalState': 'archived'
            }
        },
        {
            'project': 'project-xxx',
            'id': 'file-xxx',
            'describe': {
                'name': 'sample2-file1',
                'archivalState': 'archived'
            }
        }
    ]

    @patch('utils.dx_requests.dxpy.DXJob.add_tags')
    @patch('utils.dx_requests.dxpy.DXJob')
    @patch('utils.dx_requests.dxpy.DXFile.unarchive')
    @patch('utils.dx_requests.dxpy.DXFile')
    @patch('utils.dx_requests.sys.exit')
    def test_unarchiving_called(
            self,
            exit,
            mock_file,
            mock_unarchive,
            mock_job,
            mock_tags,
            capsys
        ):
        """
        Test that DXFile.unarchive() gets called on the provided list
        of DXFile objects
        """
        # mock_unarchive.return_value = True
        DXManage().unarchive_files(
            self.files
        )

        # lots of prints go to stdout once we have started unarchiving
        stdout = capsys.readouterr().out

        expected_stdout = [
            "Unarchiving requested for 2 files, this will take some time...",
            "The state of all files may be checked with the following command:",
            (
                "echo file-xxx file-xxx | xargs -n1 -d' ' -P32 -I{} bash -c "
                "'dx describe --json {} ' | grep archival | uniq -c"
            ),
            "This job can be relaunched once unarchiving is complete by running:",
            "dx run app-eggd_dias_batch --clone None -iunarchive=false"
        ]

        assert all(x in stdout for x in expected_stdout), (
            "stdout does not contain the expected output"
        )


    @patch('utils.dx_requests.dxpy.DXFile', side_effect=Exception('Error'))
    @patch('utils.dx_requests.sleep')
    def test_error_raised_if_unable_to_unarchive(
            self,
            mock_sleep,
            mock_dxfile
        ):
        """
        Function will try and catch up to 5 times to unarchive a file,
        if it can't unarchive a file an error should be raised. Here
        we make it raise an Exception to test it in the loop and ensure
        that it stops after failing.
        """
        with pytest.raises(
            RuntimeError,
            match=r'\[Attempt 5/5\] Error in unarchiving file: file-xxx'
        ):
           DXManage().unarchive_files(self.files)


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
            "Invalid stage folders returned for app"
        )
