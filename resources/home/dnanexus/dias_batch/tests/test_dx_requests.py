"""
The majority of functions in dx_requests.py relate to interacting with
DNAnexus via dxpy API calls to either manage data (in DXManage) or for
launching jobs (in DXExecute).
"""
from copy import deepcopy
import os
import sys
import unittest
from unittest import mock
from unittest.mock import patch

import dxpy
import pandas as pd
import pytest


sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))

from utils import utils
from utils.dx_requests import DXExecute, DXManage


class TestDXManageReadAssayConfigFile():
    """
    Tests for DXManage.read_assay_config_file()

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
    def setUp(self):
        """
        Setup our mocks
        """
        # set up patches for each sub function call in DXExecute.cnv_calling
        self.loads_patch = mock.patch('utils.dx_requests.json.loads')
        self.find_patch = mock.patch('utils.dx_requests.dxpy.find_data_objects')
        self.file_patch = mock.patch('utils.dx_requests.dxpy.DXFile')
        self.read_patch = mock.patch('utils.dx_requests.dxpy.DXFile.read')

        # create our mocks to reference
        self.mock_loads = self.loads_patch.start()
        self.mock_find = self.find_patch.start()
        self.mock_file = self.file_patch.start()
        self.mock_read = self.read_patch.start()


    def tearDown(self):
        self.mock_loads.stop()
        self.mock_find.stop()
        self.mock_file.stop()
        self.mock_read.stop()


    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        """Capture stdout to provide it to tests"""
        self.capsys = capsys


    def test_error_raised_when_path_invalid(self):
        """
        AssertionError should be raised if path param is not valid
        """
        expected_error = 'path to assay configs appears invalid: invalid_path'

        with pytest.raises(AssertionError, match=expected_error):
            DXManage().get_assay_config(path='invalid_path', assay='')


    def test_error_raised_when_no_config_files_found(self):
        """
        AssertionError should be raised if no JSON files found, patch
        the return of the dxpy.find_data_objects() call to be empty and
        check we raise an error correctly
        """
        self.mock_find.return_value = []

        expected_error = (
            'No config files found in given path: project-xxx:/test_path'
        )
        with pytest.raises(AssertionError, match=expected_error):
            DXManage().get_assay_config(path='project-xxx:/test_path', assay='')


    def test_error_raised_when_no_config_file_found_for_assay(self):
        """
        AssertionError should be raised if we find some JSON files but
        after parsing through none of them match our given assay string
        against the 'assay' key in the config
        """
        # set output of find to be minimal describe call output with
        # required keys for iterating over
        self.mock_find.return_value = [
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
        self.mock_file.return_value = dxpy.DXFile

        # patch the output from DXFile.read() to just be all the same dict
        self.mock_loads.return_value = {'assay': 'CEN', 'version': '1.0.0'}

        expected_error = (
            "No config file was found for test from project-xxx:/test_path"
        )

        with pytest.raises(AssertionError, match=expected_error):
            DXManage().get_assay_config(
                path='project-xxx:/test_path',
                assay='test'
            )


    def test_highest_version_correctly_selected(self):
        """
        Test that when multiple configs are found for an assay, the
        highest version is correctly returned. We're using
        packaging.version.Version to compare versions parsed from
        the config files so this _should_ work as we expect
        """
        # set output of find to be minimal describe call output with
        # required keys for iterating over, here we need a dict per
        # `mock_read` return values that we want to test with
        self.mock_find.return_value = [
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
        self.mock_file.return_value = dxpy.DXFile

        # patch the output from DXFile.read() to simulate looping over
        # the return of reading multiple configs
        self.mock_loads.side_effect = [
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


    def test_non_live_files_skipped(self):
        """
        Test when one or more configs are not in a live state that these
        are skipped
        """
        # minimal dxpy.find_data_objects return of a live and archived config
        self.mock_find.return_value = [
            {
                'project': 'project-xxx',
                'id': 'file-xxx',
                'describe': {
                    'name': 'config1.json',
                    'archivalState': 'live'
                }
            },
            {
                'project': 'project-xxx',
                'id': 'file-xxx',
                'describe': {
                    'name': 'config2.json',
                    'archivalState': 'archived'
                }
            },
            {
                'project': 'project-xxx',
                'id': 'file-xxx',
                'describe': {
                    'name': 'config3.json',
                    'archivalState': 'archival'
                }
            },
            {
                'project': 'project-xxx',
                'id': 'file-xxx',
                'describe': {
                    'name': 'config4.json',
                    'archivalState': 'unarchiving'
                }
            }
        ]

        # patch the DXFile object that read() gets called on
        self.mock_file.return_value = dxpy.DXFile

        # patch the output from DXFile.read() for our live config
        self.mock_loads.return_value = {
            'assay': 'test', 'version': '1.0.0'
        }

        DXManage().get_assay_config(
            path='project-xxx:/test_path',
            assay='test'
        )

        stdout = self.capsys.readouterr().out

        expected_warning = (
            "Config file not in live state - will not be used: "
            "config2.json (file-xxx)"
        )

        assert expected_warning in stdout, (
            "Warning not printed for archived file"
        )


    def test_multiple_files_raises_error(self):
        """
        Test that when more than one file found for same config version
        that a RuntimeError is raised since we can't unambiguously tell
        which file to use
        """
        # set output of find to be minimal describe call output with
        # required keys for iterating over, here we need a dict per
        # `mock_read` return values that we want to test with
        self.mock_find.return_value = [
            {
                'project': 'project-xxx',
                'id': 'file-xxx',
                'describe' : {
                    'name': 'config1.json',
                    'archivalState': 'live'
                }
            },
            {
                'project': 'project-xxx',
                'id': 'file-yyy',
                'describe' : {
                    'name': 'config2.json',
                    'archivalState': 'live'
                }
            },
            {
                'project': 'project-xxx',
                'id': 'file-zzz',
                'describe' : {
                    'name': 'config3.json',
                    'archivalState': 'live'
                }
            }
        ]

        # patch the DXFile object that read() gets called on
        self.mock_file.return_value = dxpy.DXFile

        # patch the output from DXFile.read() to simulate looping over
        # the return of reading multiple configs with 2 of the same version
        self.mock_loads.side_effect = [
            {'assay': 'test', 'version': '1.0.0'},
            {'assay': 'test', 'version': '1.1.0'},
            {'assay': 'test', 'version': '1.1.0'},
        ]

        expected_error = (
            "Error: more than one file found for highest version of test "
            "configs. Files found:\\n\\tconfig2.json \(file-yyy\)\\n\\t"
            "config3.json \(file-zzz\)"
        )

        with pytest.raises(RuntimeError, match=expected_error):
            DXManage().get_assay_config(
                path='project-xxx:/test_path',
                assay='test'
            )


class TestDXManageGetFileProjectContext():
    """
    Tests for DXManage.get_file_project_context()

    Function takes a DXFile ID and returns a project ID in which
    the file has been found in a live state
    """
    @patch('utils.dx_requests.dxpy.DXFile.list_projects')
    @patch('utils.dx_requests.dxpy.DXFile.describe')
    @patch('utils.dx_requests.dxpy.DXFile')
    def test_no_live_files(
            self,
            mock_file,
            mock_describe,
            mock_list
        ):
        """
        Test that when no files in a live state are found that an
        AssertionError is raised
        """
        # patch the DXFile object that read() gets called on
        mock_file.return_value = dxpy.DXFile
        mock_list.return_value = {
            'project-xxx': 'CONTRIBUTE',
            'project-yyy': 'CONTRIBUTE'
        }

        # mock describing each project:file context found
        mock_describe.side_effect = [
            {
                'project': 'project-xxx',
                'id': 'file-xxx',
                'archivalState': 'archived'
            },
            {
                'project': 'project-yyy',
                'id': 'file-xxx',
                'archivalState': 'archival'

            }
        ]

        correct_error = 'No live files could be found for the ID: file-xxx'

        with pytest.raises(AssertionError, match=correct_error):
            DXManage().get_file_project_context(file='file-xxx')

    @patch('utils.dx_requests.dxpy.DXFile.list_projects')
    @patch('utils.dx_requests.dxpy.DXFile.describe')
    @patch('utils.dx_requests.dxpy.DXFile')
    def test_live_files(
        self,
        mock_file,
        mock_describe,
        mock_list,
        capsys
    ):
        """
        Test when some live files are found, we correctly return the
        first one to use as the project context
        """
        # patch the DXFile object that read() gets called on
        mock_file.return_value = dxpy.DXFile
        mock_list.return_value = {
            'project-xxx': 'CONTRIBUTE',
            'project-yyy': 'CONTRIBUTE'
        }

        # mock describing each project:file context found
        mock_describe.side_effect = [
            {
                'project': 'project-xxx',
                'id': 'file-xxx',
                'archivalState': 'live'
            },
            {
                'project': 'project-yyy',
                'id': 'file-xxx',
                'archivalState': 'live'

            }
        ]

        returned = DXManage().get_file_project_context(file='file-xxx')

        errors = []

        # check we print what we expect
        stdout = capsys.readouterr().out
        expected_print = (
            'Found file-xxx in 2 projects, using project-xxx as project context'
        )

        if expected_print not in stdout:
            errors.append('Did not print expected file project context')

        if not returned['project'] == 'project-xxx':
            errors.append('Incorrect file context returned')

        assert not errors, errors


class TestDXManageFindFiles():
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


    @patch('utils.dx_requests.dxpy.find_data_objects')
    def test_archived_files_flagged_in_logs(self, mock_find, capsys):
        """
        If any files are found to be not live, a warning should be added
        to the logs, and then this will raises an error when
        DXManage.check_archival_state is called before running any jobs
        """
        mock_find.return_value = [
            {
                'project': 'project-xxx',
                'id': 'file-xxx',
                'describe': {
                    'name': 'file1',
                    'archivalState': 'archived',
                    'folder': '/path_to_files/subdir1/app1'
                }
            }
        ]

        DXManage().find_files(
            path='project-xxx:/path_to_files/'
        )

        stdout = capsys.readouterr().out
        expected_warning = (
            'WARNING: some files found are in an archived state, if these are '
            'for samples to be analysed this will raise an error...'
            '\n[\n⠀⠀"file1 (file-xxx)"\n]'
        )

        assert expected_warning in stdout, (
            'Expected warning for archived files not printed'
        )


class TestDXManageReadDXfile():
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
        Test when we provide file ID as just 'file-xxx' that we call
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
        Test when we provide file ID as just 'file-xxx' that we call
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


    @patch('utils.dx_requests.dxpy.DXFile')
    def test_trailing_blank_line_removed(self, mock_file):
        """
        If the file has a blank line at the end this would result in
        the file being read into a list with an empty string at the
        end => test that we correctly remove this
        """
        mock_file.return_value.read.return_value = 'line1\nline2\nline3\n'

        contents = DXManage().read_dxfile(file='project-xxx:file-xxx')

        assert contents == ['line1', 'line2', 'line3']


@patch('utils.dx_requests.DXManage.find_files')
@patch('utils.dx_requests.DXManage.check_archival_state')
class TestCheckAllFilesArchivalState(unittest.TestCase):
    """
    Tests for dx_requests.check_all_files_archival_state

    Function takes patterns per running mode of required file types to
    check archival state of, and queries the given path for all files
    and then checks the archival state.
    """
    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        """Capture stdout to provide it to tests"""
        self.capsys = capsys


    def test_default_patterns_used_if_no_default_provided(
        self, mock_archive, mock_find
    ):
        """
        Test if no pattern is provided from the config that the default
        patterns from utils.defaults is used.

        We will use calls to dx_requests.find_files as a proxy for the
        config patterns being used, as we expected 6 calls for the 4
        running modes to be made (4x per sample and 2x per run patterns).
        """
        DXManage().check_all_files_archival_state(
            patterns=None,
            samples=['sample_1', 'sample_2'],
            path='project-xxx:/',
            unarchive=False,
            modes={
                'cnv_reports': True,
                'snv_reports': True,
                'mosaic_reports': True,
                'artemis': True
            }
        )

        assert mock_find.call_count == 6, (
            'incorrect number of calls to dx_requests.find_files'
        )


    def test_running_mode_check_skipped_if_not_selected(
        self, mock_archive, mock_find
    ):
        """
        Test if when a running mode has not been selected (i.e. not
        running CNV reports) that the checks for these file types are
        not run.
        """
        DXManage().check_all_files_archival_state(
            patterns=None,
            samples=['sample_1', 'sample_2'],
            path='project-xxx:/',
            unarchive=False,
            modes={
                'cnv_reports': False,
                'snv_reports': True,
                'mosaic_reports': True,
                'artemis': True
            }
        )

        with self.subTest('expected message not in stdout'):
            expected_stdout = (
                "Running mode cnv_reports not selected, skipping file check"
            )

            assert expected_stdout in self.capsys.readouterr().out

        with self.subTest('incorrect calls made to dx_requests.find_files'):
            assert mock_find.call_count == 4


    def test_correct_patterns_provided_for_each_mode(
        self, mock_archive, mock_find
    ):
        """
        Test that for each running mode the correct patterns are provided
        when doing the searching
        """
        DXManage().check_all_files_archival_state(
            patterns=None,
            samples=['sample_1', 'sample_2'],
            path='project-xxx:/',
            unarchive=False,
            modes={
                'cnv_reports': True,
                'snv_reports': True,
                'mosaic_reports': True,
                'artemis': True
            }
        )

        # define what patterns we expect the function to provide to
        # each call to dx_requests.find_files for each running mode
        expected_called_patterns = {
            'cnv_reports_sample': (
                    'sample_1.*_segments.vcf$|sample_2.*_segments.vcf$'
            ),
            'cnv_reports_run': '_excluded_intervals.bed$',
            'snv_reports': (
                'sample_1.*_markdup_recalibrated_Haplotyper.vcf.gz$|'
                'sample_1.*per-base.bed.gz$|sample_1.*reference_build.txt$|'
                'sample_2.*_markdup_recalibrated_Haplotyper.vcf.gz$|'
                'sample_2.*per-base.bed.gz$|sample_2.*reference_build.txt$'
                ),
            'mosaic_reports': (
                'sample_1.*_markdup_recalibrated_tnhaplotyper2.vcf.gz|'
                'sample_1.*per-base.bed.gz$|sample_1.*reference_build.txt$|'
                'sample_2.*_markdup_recalibrated_tnhaplotyper2.vcf.gz|'
                'sample_2.*per-base.bed.gz$|sample_2.*reference_build.txt$'
            ),
            'artemis_sample': (
                'sample_1.*bam$|sample_1.*bam.bai$|'
                'sample_1.*_copy_ratios.gcnv.bed.gz$|'
                'sample_1.*_copy_ratios.gcnv.bed.gz.tbi$|'
                'sample_2.*bam$|sample_2.*bam.bai$|'
                'sample_2.*_copy_ratios.gcnv.bed.gz$|'
                'sample_2.*_copy_ratios.gcnv.bed.gz.tbi$'
            ),
            'artemis_run': '-multiqc.html'
        }

        called_patterns = [x[1]['pattern'] for x in mock_find.call_args_list]

        assert sorted(expected_called_patterns.values()) == sorted(called_patterns), (
            'incorrect patterns provided to dx_requests.find_files'
        )


    def test_call_to_check_archival_state_correct(
        self, mock_archive, mock_find
    ):
        """
        Test that when we've found files for samples for each running mode,
        that these are all correctly passed to
        dx_requests.check_archival_state to actually check if they're archived
        """
        # define what files each call to dx_requests.find_files should
        # return, will be called twice for CNV reports (per sample then
        # per run), then once for each other mode
        mock_find.side_effect = [
            ['sample1_segments.vcf', 'sample2_segments.vcf'],
            ['myRun_excluded_intervals.bed'],
            [
                'sample1_markdup_recalibrated_Haplotyper.vcf.gz',
                'sample2_markdup_recalibrated_Haplotyper.vcf.gz',
                'sample1_per-base.bed.gz',
                'sample2_reference_build.txt',
                'sample1_per-base.bed.gz',
                'sample2_reference_build.txt'
            ],
            [
                'sample1_markdup_recalibrated_tnhaplotyper2.vcf.gz',
                'sample2_markdup_recalibrated_tnhaplotyper2.vcf.gz',
                'sample1_per-base.bed.gz',
                'sample2_reference_build.txt',
                'sample1_per-base.bed.gz',
                'sample2_reference_build.txt'
            ],
            [
                'sample1_bam$',
                'sample1_bam.bai$',
                'sample1_copy_ratios.gcnv.bed$',
                'sample1_copy_ratios.gcnv.bed.tbi$',
                'sample2_bam$',
                'sample2_bam.bai$',
                'sample2_copy_ratios.gcnv.bed$',
                'sample2_copy_ratios.gcnv.bed.tbi$'
            ],
            ['002_myRun-multiqc.html']
        ]

        DXManage().check_all_files_archival_state(
            patterns=None,
            samples=['sample_1', 'sample_2'],
            path='project-xxx:/',
            unarchive=False,
            modes={
                'cnv_reports': True,
                'snv_reports': True,
                'mosaic_reports': True,
                'artemis': True
            }
        )

        # we expect to pass a single level list of all above files to
        # dx_requests.check_archival_state
        expected_sample_files = [
                'sample1_segments.vcf', 'sample2_segments.vcf',
                'sample1_markdup_recalibrated_Haplotyper.vcf.gz',
                'sample2_markdup_recalibrated_Haplotyper.vcf.gz',
                'sample1_per-base.bed.gz',
                'sample2_reference_build.txt',
                'sample1_per-base.bed.gz',
                'sample2_reference_build.txt',
                'sample1_markdup_recalibrated_tnhaplotyper2.vcf.gz',
                'sample2_markdup_recalibrated_tnhaplotyper2.vcf.gz',
                'sample1_per-base.bed.gz',
                'sample2_reference_build.txt',
                'sample1_per-base.bed.gz',
                'sample2_reference_build.txt',
                'sample1_bam$',
                'sample1_bam.bai$',
                'sample1_copy_ratios.gcnv.bed$',
                'sample1_copy_ratios.gcnv.bed.tbi$',
                'sample2_bam$',
                'sample2_bam.bai$',
                'sample2_copy_ratios.gcnv.bed$',
                'sample2_copy_ratios.gcnv.bed.tbi$'
        ]

        expected_run_files = [
            '002_myRun-multiqc.html',
            'myRun_excluded_intervals.bed'
        ]

        with self.subTest('wrong sample files passed to check archival state'):
            assert sorted(mock_archive.call_args[1]['sample_files']) == \
                sorted(expected_sample_files)

        with self.subTest('wrong run files passed to check archival state'):
            assert sorted(mock_archive.call_args[1]['non_sample_files']) == \
                expected_run_files


    def test_unarchive_passed_to_check_archival_state(
        self, mock_archive, mock_find
    ):
        """
        Test that the unarchive param passed to check_all_files_archival_
        state is passed through to dx_requests.check_archival_state
        """
        # set some return value so check_archival_state will get called
        mock_find.return_value = ['foo']

        with self.subTest('check unarchive False'):
            DXManage().check_all_files_archival_state(
                patterns=None,
                samples=['sample_1', 'sample_2'],
                path='project-xxx:/',
                unarchive=False,
                modes={
                    'cnv_reports': True,
                    'snv_reports': True,
                    'mosaic_reports': True,
                    'artemis': True
                }
            )

            # should be passed through as False
            assert mock_archive.call_args[1]['unarchive'] == False

        with self.subTest('check unarchive False'):
            DXManage().check_all_files_archival_state(
                patterns=None,
                samples=['sample_1', 'sample_2'],
                path='project-xxx:/',
                unarchive=True,
                modes={
                    'cnv_reports': True,
                    'snv_reports': True,
                    'mosaic_reports': True,
                    'artemis': True
                }
            )

            # should be passed through as True
            assert mock_archive.call_args[1]['unarchive'] == True

    @patch('utils.dx_requests.exit')
    @patch('utils.dx_requests.dxpy.DXJob')
    def test_unarchive_only_correctly_tags_job_and_exits(
        self, mock_job, mock_exit, mock_archive, mock_find
    ):
        """
        Test that when we specify unarchive_only and there are no files
        to unarchive (which would exit from dx_requests.DXManage.check_
        file_archival_status) that we exit here with a zero exit code
        """
        DXManage().check_all_files_archival_state(
            patterns=None,
            samples=['sample_1', 'sample_2'],
            path='project-xxx:/',
            unarchive=False,
            unarchive_only=True,
            modes={
                'cnv_reports': True,
                'snv_reports': True,
                'mosaic_reports': True,
                'artemis': True
            }
        )

        with self.subTest('stdout not correct'):
            expected_stdout = (
                '-iunarchive_only set and no files in archived state '
                '- exiting now'
            )
            assert expected_stdout in self.capsys.readouterr().out

        with self.subTest('DXJob.add_tags not called'):
            assert mock_job.return_value.add_tags.call_count == 1

class TestDXManageCheckArchivalState(unittest.TestCase):
    """
    Tests for DXManage.check_archival_state()

    Function takes in a list of DXFileObjects (and optionally a list
    of sample names to filter by), and checks the archival state of
    the files to ensure all are live before launching jobs
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


    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        """Capture stdout to provide it to tests"""
        self.capsys = capsys


    def test_all_live(self):
        """
        Test no error is raised when all provided files are live
        """
        DXManage().check_archival_state(
            sample_files=self.files,
            unarchive=False
        )

        # since we don't explicitly return anything when there are no
        # archived files, check stdout for expected string printed
        # to ensure the function passed through all checks to the end
        stdout = self.capsys.readouterr().out

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
                sample_files=self.files_w_archive,
                unarchive=False
            )


    def test_archived_files_filtered_out_when_not_in_sample_list(self):
        """
        Test when a list of sample names is provided that any files for other
        samples are filtered out, we will test this by adding an archived file
        for a non-matching sample and checking it is removed
        """
        # provide list of sample names to filter by
        DXManage().check_archival_state(
            sample_files=self.files_w_archive,
            unarchive=False,
            samples=['sample1', 'sample2', 'sample3', 'sample4']
        )

        # since we don't explicitly return anything for all being live check
        # stdout for expected string printed to ensure we got where we expect
        stdout = self.capsys.readouterr().out

        assert 'No required files in archived state' in stdout, (
            'Expected print for all live files not in captured stdout'
        )


    def test_archived_files_kept_when_in_sample_list(self):
        """
        Test when we have some archived files and a provided list of samples,
        and the archived files are for those selected samples
        """
        with pytest.raises(
            RuntimeError,
            match='Files required for analysis archived'
        ):
            # provide list of sample names to filter by, sample5 has
            # archived file and unarchive=False => should raise error
            DXManage().check_archival_state(
                sample_files=self.files_w_archive,
                unarchive=False,
                samples=['sample5']
            )


    def test_archived_non_sample_file_kept_when_sample_list_given(self):
        """
        Can pass a list of sample files plus non-sample file(s) (i.e.
        intervals bed file from CNV calling), when the samples param is
        also specified (i.e. list of sample names), ensure we don't wrongly
        remove the non sample files
        """
        non_sample_archived_file = [
            {
                'id': 'file-zzz',
                'describe': {
                    'name': 'some_other_run_level_file.bed',
                    'archivalState': 'archived'
                }
            }
        ]

        # test archived non sample file correctly raises error when
        # provided with sample files and samples list
        expected_error = "Files required for analysis archived"
        with self.subTest():
            with pytest.raises(RuntimeError, match=expected_error):
                DXManage().check_archival_state(
                    sample_files=self.files_w_archive,
                    non_sample_files=non_sample_archived_file,
                    unarchive=False,
                    samples=['sample5']
                )

            # actually test the bed file is flagged
            archived_bed_stdout = (
                "some_other_run_level_file.bed (file-zzz) - archived"
            )

            assert archived_bed_stdout in self.capsys.readouterr().out, (
                'Archived bed not correctly identified as archived'
            )

        # test archived non sample file correctly raises error when
        # NOT provided with other files
        expected_error = "Files required for analysis archived"
        with self.subTest():
            with pytest.raises(RuntimeError, match=expected_error):
                DXManage().check_archival_state(
                    non_sample_files=non_sample_archived_file,
                    unarchive=False
                )

            # actually test the bed file is flagged
            archived_bed_stdout = (
                "some_other_run_level_file.bed (file-zzz) - archived"
            )

            assert archived_bed_stdout in self.capsys.readouterr().out, (
                'Archived bed not correctly identified as archived'
            )


    def test_error_raised_when_non_live_files_can_not_be_unarchived(self):
        """
        When non-live files are found their archivalState is checked and
        those in 'unarchiving' state are removed from those to request
        unarchiving on, as this would raise an error.

        If no files are left to unarchive after removing these an error
        should be raised as we are in a state of not being able to run
        jobs and also not able to call unarchiving
        """
        # minimal test file objects where one file is unarchiving and
        # another is archiving
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
                    'archivalState': 'unarchiving'
                }
            }
        ]

        with pytest.raises(
            RuntimeError,
            match='non-live files not in a state that can be unarchived'
        ):
            DXManage().check_archival_state(
                sample_files=files,
                unarchive=True
            )


    @patch('utils.dx_requests.DXManage.unarchive_files')
    def test_unarchive_files_called_when_specified(self, mock_unarchive):
        """
        Test when we have archived files and unarchive=True specified that
        we call the function to start unarchiving
        """
        DXManage().check_archival_state(
            sample_files=self.files_w_archive,
            unarchive=True
        )

        assert mock_unarchive.called, (
            'DXManage.unarchive_files not called for unarchive=True'
        )



class TestDXManageUnarchiveFiles(unittest.TestCase):
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
    @patch('utils.dx_requests.dxpy.api.project_unarchive')
    @patch('utils.dx_requests.sys.exit')
    def test_unarchiving_called(
            self,
            exit,
            mock_unarchive,
            mock_job,
            mock_tags
        ):
        """
        Test that dxpy.api.project_unarchive() gets called on
        the provided list of DXFile objects
        """
        DXManage().unarchive_files(
            self.files
        )

        mock_unarchive.assert_called()


    @patch('utils.dx_requests.dxpy.DXJob.add_tags')
    @patch('utils.dx_requests.dxpy.DXJob')
    @patch('utils.dx_requests.dxpy.api.project_unarchive')
    @patch('utils.dx_requests.sys.exit')
    def test_unarchive_called_per_project(
            self,
            exit,
            mock_unarchive,
            mock_job,
            mock_tags
        ):
        """
        If files found are in more than one project the function
        will loop over each set of files per project, test that this
        correctly happens where files are in 3 projects
        """
        # minimal example of 3 files in 3 separate projects
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
                'project': 'project-yyy',
                'id': 'file-yyy',
                'describe': {
                    'name': 'sample2-file1',
                    'archivalState': 'archived'
                }
            },
            {
                'project': 'project-zzz',
                'id': 'file-zzz',
                'describe': {
                    'name': 'sample2-file1',
                    'archivalState': 'archived'
                }
            }
        ]

        DXManage().unarchive_files(files)

        self.assertEqual(mock_unarchive.call_count, 3)


    @patch(
        'utils.dx_requests.dxpy.api.project_unarchive',
        side_effect=Exception('someDNAnexusAPIError')
    )
    def test_error_raised_if_unable_to_unarchive(
            self,
            mock_unarchive
        ):
        """
        If any error is raised during calling dxpy.api.project_unarchive
        it will be caught and raise a RuntimeError, test that if an
        Exception is raised we get the expected error message
        """
        with pytest.raises(
            RuntimeError,
            match='Error unarchiving files'
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
        Test when an applet is included as a stage that the path is
        correctly set, applets are treated differently to apps as the
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
            time_stamp='010123_1303',
            name='workflow1_SNV'
        )

        correct_stage_folder = {
            "stage1": "/some_output_path/workflow1_SNV/010123_1303/applet1-v1.2.3/"
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
            time_stamp='010123_1303',
            name='workflow1'
        )

        assert correct_stage_folder == returned_stage_folder, (
            "Invalid stage folders returned for app"
        )


class TestDXExecuteCNVCalling(unittest.TestCase):
    """
    Tests for DXExecute.cnv_calling

    This is the main function that calls all others to set up inputs for
    CNV calling and runs the app. The majority of what is called here is
    already covered by other unit tests, and therefore a lot will be
    mocked where called functions make dx requests etc themselves.

    We will mostly be testing that where the different inputs are given,
    that expected prints go to stdout since that is the most we can test
    """
    config = {
        'modes': {
            'cnv_call': {
                'inputs': {
                    'bambais': {
                        'folder': '/sentieon-dnaseq',
                        'name': '.bam$|.bam.bai$'
                    }
                }
            }
        }
    }

    def setUp(self):
        """
        Set up test class wide patches
        """
        # set up patches for each sub function call in DXExecute.cnv_calling
        self.path_patch = mock.patch('utils.dx_requests.make_path')
        self.find_patch = mock.patch('utils.dx_requests.DXManage.find_files')
        self.check_archival_state_patch = mock.patch(
            'utils.dx_requests.DXManage.check_archival_state'
        )
        self.describe_patch = mock.patch('utils.dx_requests.dxpy.describe')
        self.dxapp_patch = mock.patch('utils.dx_requests.dxpy.DXApp')
        self.run_patch = mock.patch('utils.dx_requests.dxpy.run')
        self.job_patch = mock.patch('utils.dx_requests.dxpy.DXJob')
        self.wait_patch = mock.patch('utils.dx_requests.dxpy.bindings.DXJob.wait_on_done')

        # create our mocks to reference
        self.mock_path = self.path_patch.start()
        self.mock_find = self.find_patch.start()
        self.mock_archive = self.check_archival_state_patch.start()
        self.mock_describe = self.describe_patch.start()
        self.mock_dxapp = self.dxapp_patch.start()
        self.mock_run = self.run_patch.start()
        self.mock_job = self.job_patch.start()
        self.mock_wait = self.wait_patch.start()

        # Below we define some returns in expected format to use for the mocks

        # utils.make_path called twice, once to get path for searching for
        # BAM files then again for setting app output
        self.mock_path.side_effect = [
            'project-GZ025k04VjykZx3bJ7YP837:/output/CEN-230719_1604/sentieon',
            (
                'project-GZ025k04VjykZx3bJ7YP837:/output/CEN-230719_1604/'
                'GATK_gCNV_call-1.2.3/0925-17'
            )
        ]

        # mocked return of calling DXManage.find_files to search for input BAMs
        self.mock_find.return_value = [
            {
                'id': 'file-xxx',
                'describe': {
                    'name': 'sample1.bam'
                }
            },
            {
                'id': 'file-xxx',
                'describe': {
                    'name': 'sample1.bam.bai'
                }
            },
            {
                'id': 'file-xxx',
                'describe': {
                    'name': 'sample2.bam'
                }
            },
            {
                'id': 'file-xxx',
                'describe': {
                    'name': 'sample2.bam.bai'
                }
            },
            {
                'id': 'file-xxx',
                'describe': {
                    'name': 'sample3.bam'
                }
            },
            {
                'id': 'file-xxx',
                'describe': {
                    'name': 'sample3.bam.bai'
                }
            }
        ]

        # first dxpy.describe call is on the project ID to get the project
        # name, second is on app ID and third is on job ID
        # patch in minimal responses with required keys
        self.mock_describe.side_effect = [
            {
                'name': '002_test_project'
            },
            {
                'name': 'GATK_gCNV_call',
                'version': '1.2.3'
            },
            {
                'id': 'job-GXvQjz04YXKx5ZPjk36B17j2'
            }
        ]


    def tearDown(self):
        """
        Remove test class wide patches
        """
        self.mock_path.stop()
        self.mock_find.stop()
        self.mock_archive.stop()
        self.mock_describe.stop()
        self.mock_dxapp.stop()
        self.mock_run.stop()
        self.mock_job.stop()
        self.mock_wait.stop()


    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        """Capture stdout to provide it to tests"""
        self.capsys = capsys


    def test_cnv_call(self):
        """
        Test with everything patched that no errors are raised
        """
        DXExecute().cnv_calling(
            config=deepcopy(self.config),
            single_output_dir='',
            exclude=[],
            start='',
            wait=False,
            unarchive=False
        )


    def test_wait_on_done(self):
        """
        Test if wait=True is specified that the app will be held until
        calling completes
        """
        DXExecute().cnv_calling(
            config=deepcopy(self.config),
            single_output_dir='',
            exclude=[],
            start='',
            wait=True,
            unarchive=False
        )

        stdout = self.capsys.readouterr().out

        assert 'Holding app until CNV calling completes...' in stdout, (
            'App not waiting with wait=True specified'
        )


    def test_exclude(self):
        """
        Test when exclude samples is specified that these are used
        """
        DXExecute().cnv_calling(
            config=deepcopy(self.config),
            single_output_dir='',
            exclude=['sample2', 'sample3'],
            start='',
            wait=False,
            unarchive=False
        )

        stdout = self.capsys.readouterr().out

        correct_exclude = (
            '2 .bam/.bai files after excluding:\n\tsample1.bam\n\tsample1.bam.bai'
        )

        assert correct_exclude in stdout, (
            'exclude samples incorrect'
        )


    def test_exclude_invalid_sample(self):
        """
        Test when exclude samples is specified with an sample name that
        has no BAM file present a RuntimeError is correctly raised from
        the call to utils.check_exclude_samples
        """
        correct_error = (
            "samples provided to exclude from CNV calling not valid: "
            r"\['sample1000'\]"
        )

        with pytest.raises(RuntimeError, match=correct_error):
            DXExecute().cnv_calling(
                config=deepcopy(self.config),
                single_output_dir='',
                exclude=['sample1000'],
                start='',
                wait=False,
                unarchive=False
            )


    def test_excluded_files_returned_correct_format(self):
        """
        Test that the files excluded from CNV calling are returned as a
        list of file name strings
        """
        _, excluded = DXExecute().cnv_calling(
            config=deepcopy(self.config),
            single_output_dir='',
            exclude=['sample2', 'sample3'],
            start='',
            wait=False,
            unarchive=False
        )

        correct_exclude = [
            'sample2.bam', 'sample2.bam.bai', 'sample3.bam', 'sample3.bam.bai'
        ]

        self.assertEqual(excluded, correct_exclude)


    def test_correct_error_raised_on_calling_failing(self):
        """
        If error raised during CNV calling whilst waiting to complete,
        test this is caught and exits the app
        """
        # patch return of DXJob to be an empty DXJob object, and set the
        # error to be raised from DXJob.wait_on_done()
        self.mock_job.return_value = dxpy.bindings.DXJob(dxid='localjob-')
        self.mock_wait.side_effect = dxpy.exceptions.DXJobFailureError(
            'oh no :sadpanda:')

        with pytest.raises(
            dxpy.exceptions.DXJobFailureError, match='oh no :sadpanda:'
        ):
            DXExecute().cnv_calling(
                config=deepcopy(self.config),
                single_output_dir='',
                exclude=[],
                start='',
                wait=True,
                unarchive=False
            )


    def test_assertion_error_raised_on_no_files_found(self):
        """
        If no BAM files are found after searching the given directory
        an AssertionError should be raised, test this happens
        """
        self.mock_find.return_value = []

        with pytest.raises(
            AssertionError,
            match='No BAM files found for CNV calling'
        ):
            DXExecute().cnv_calling(
                config=deepcopy(self.config),
                single_output_dir='',
                exclude=[],
                start='',
                wait=True,
                unarchive=False
            )


class TestDXExecuteReportsWorkflow(unittest.TestCase):
    """
    Unit tests for DXExecute.reports_workflow

    This is the chonky function that handles setting up and launching all
    reports workflows, it can be run in 3 different modes (CNV, SNV or
    mosaic), and for each different functions and parameters are set.

    There are a lot of calls to own functions inside here (such as
    finding files and checking archival state), so there is going to
    be a lot of mocking and patching returns etc. to test all the
    conditional behaviour.
    """
    # example minimal assay config with required keys for testing
    assay_config = {
        "assay": "CEN",
        "version": "2.2.0",
        "cnv_call_app_id": "app-GJZVB2840KK0kxX998QjgXF0",
        "snv_report_workflow_id": "workflow-GXzkfYj4QPQp9z4Jz4BF09y6",
        "cnv_report_workflow_id": "workflow-GXzvJq84XZB1fJk9fBfG88XJ",
        "name_patterns": {
            "Epic": "^[\\d\\w]+-[\\d\\w]+",
            "Gemini": "^X[\\d]+"
        },
        "modes": {
            "cnv_reports": {
                "inputs": {
                    "stage-cnv_vep.vcf": {
                        "folder": "CNV_vcfs",
                        "name": "_segments.vcf$"
                    }
                }
            },
            "snv_reports": {
                "inputs": {
                    "stage-rpt_vep.vcf": {
                        "folder": "sentieon-dnaseq",
                        "name": ".vcf"
                    },
                    "stage-rpt_athena.mosdepth_files": {
                        "folder": "eggd_mosdepth",
                        "name": "per-base.bed.gz$|reference.txt$"
                    }
                }
            }
        }
    }

    # minimal manifest with parsed in indications and panels
    manifest = {
        "X1234": {
            "manifest_source": "Epic",
            "tests": [["R207.1"]],
            "panels": [
                ["Inherited ovarian cancer (without breast cancer)_4.0"]
            ],
            "indications": [[
                "R207.1_Inherited ovarian cancer (without breast cancer)_P"
            ]]
        },
        "X5678": {
            "manifest_source": "Epic",
            "tests": [["R134.1"]],
            "panels": [
                ["Familial hypercholesterolaemia (GMS)_2.0"]
            ],
            "indications": [
                ["R134.1_Familial hypercholesterolaemia_P"]
            ]
        },
    }


    def setUp(self):
        """
        Set up all of the functions to mock and some of their patched in
        return values that can be applied to multiple tests
        """
        self.find_patch = mock.patch('utils.dx_requests.DXManage.find_files')
        self.job_patch = mock.patch('utils.dx_requests.dxpy.DXJob')
        self.filter_manifest_patch = mock.patch(
            'utils.dx_requests.filter_manifest_samples_by_files'
        )
        self.archival_patch = mock.patch(
            'utils.dx_requests.DXManage.check_archival_state'
        )
        self.output_folders_patch = mock.patch(
            'utils.dx_requests.DXManage.format_output_folders'
        )
        self.path_patch = mock.patch('utils.dx_requests.make_path')
        self.index_patch = mock.patch('utils.dx_requests.check_report_index')
        self.workflow_patch = mock.patch('utils.dx_requests.dxpy.DXWorkflow')
        self.describe_patch = mock.patch('utils.dx_requests.dxpy.describe')
        self.timer_patch = mock.patch('utils.dx_requests.timer')


        self.mock_find = self.find_patch.start()
        self.mock_job = self.job_patch.start()
        self.mock_filter_manifest = self.filter_manifest_patch.start()
        self.mock_archival = self.archival_patch.start()
        self.mock_output_folders = self.output_folders_patch.start()
        self.mock_path = self.path_patch.start()
        self.mock_index = self.index_patch.start()
        self.mock_workflow = self.workflow_patch.start()
        self.mock_describe = self.describe_patch.start()
        self.mock_timer = self.timer_patch.start()


        # Below are some generalised expected returns for each of the
        # function calls, these will be patched over individually to
        # adjust for expected environment of each test

        # mock of filtering manifest where no invalid samples found
        self.mock_filter_manifest.return_value = [
            self.manifest,
            [],
            []
        ]

        # patch of dxpy.describe when called on workflow to return name
        # for setting output paths and job names
        self.mock_describe.return_value = {
            'name': 'reports_workflow'
        }

        # patch of adding vcfs to the manifest (i.e. manifest with describe
        # output of the vcf file added to the manifest under 'vcf' key)
        self.mock_filter_manifest.return_value = [
            {
                "X1234": {
                    "manifest_source": "Epic",
                    "tests": [["R207.1"]],
                    "panels": [
                        ["Inherited ovarian cancer (without breast cancer)_4.0"]
                    ],
                    "indications": [[
                        "R207.1_Inherited ovarian cancer (without breast cancer)_P"
                    ]],
                    "vcf": [
                        {
                            'project': 'project-xxx',
                            'id': 'file-xxx',
                            'describe': {
                                'name': 'X1234_markdup.vcf'
                            }
                        }
                    ],
                    "mosdepth": [
                        {
                            'project': 'project-xxx',
                            'id': 'file-xxx',
                            'describe': {
                                'name': 'X1234.per-base.bed.gz'
                            }
                        }
                    ]
                },
                "X5678": {
                    "manifest_source": "Epic",
                    "tests": [["R134.1"]],
                    "panels": [
                        ["Familial hypercholesterolaemia (GMS)_2.0"]
                    ],
                    "indications": [
                        ["R134.1_Familial hypercholesterolaemia_P"]
                    ],
                    "vcf": [
                        {
                            'project': 'project-xxx',
                            'id': 'file-xxx',
                            'describe': {
                                'name': 'X5678_markdup.vcf'
                            }
                        }
                    ],
                    "mosdepth": [
                        {
                            'project': 'project-xxx',
                            'id': 'file-xxx',
                            'describe': {
                                'name': 'X5678.per-base.bed.gz'
                            }
                        },
                    ]
                },
            },
            [],
            []
        ]


    def tearDown(self):
        self.mock_find.stop()
        self.mock_job.stop()
        self.mock_filter_manifest.stop()
        self.mock_archival.stop()
        self.mock_output_folders.stop()
        self.mock_path.stop()
        self.mock_index.stop()
        self.mock_workflow.stop()
        self.mock_describe.stop()
        self.mock_timer.stop()

    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        """Capture stdout to provide it to tests"""
        self.capsys = capsys


    def test_error_raised_if_name_pattern_missing_from_config(self):
        """
        Test when 'name_patterns' parsed from assay config file doesn't
        contain a match to the manifest source, an error is raised.

        Manifest source is used to select from the name_patterns dict, and
        expects to have Epic or Gemini as keys to use
        """
        with pytest.raises(
            RuntimeError,
            match='Unable to correctly parse manifest source. Parsed:'
        ):
            DXExecute().reports_workflow(
                mode='CNV',
                workflow_id='workflow-GXzvJq84XZB1fJk9fBfG88XJ',
                single_output_dir='/path_to_single/',
                manifest={},
                config={},
                start='230925_0943',
                name_patterns={},
                call_job_id='job-QaTZ9qEwkEsovKLs14DSdNqb'
            )


    def test_xlsx_reports_found(self):
        """
        Test if xlsx reports returned from DXManage.find_files that these
        are formatted correctly for use.

        n.b. here we're setting the mode to something invalid to stop the
        function going further and letting it raise a RuntimeError after
        the xlsx reports have been parsed, so we don't have to patch lots
        more for this test (mainly for laziness)
        """
        # minimal set of xlsx reports found
        self.mock_find.return_value = [
            {
                'id': 'file-xxx',
                'describe': {
                    'name': 'sample1.xlsx'
                }
            },
            {
                'id': 'file-yyy',
                'describe': {
                    'name': 'sample2.xlsx'
                }
            }
        ]

        with pytest.raises(
            RuntimeError,
            match='Invalid mode set for running reports: test'
        ):
            DXExecute().reports_workflow(
                mode='test',
                workflow_id='workflow-GXzvJq84XZB1fJk9fBfG88XJ',
                single_output_dir='/path_to_single/',
                manifest={'sample1': {'manifest_source': 'Epic'}},
                config=self.assay_config,
                start='230925_0943',
                name_patterns={'Epic': '[\d\w]+-[\d\w]+'},
                call_job_id='job-QaTZ9qEwkEsovKLs14DSdNqb'
            )

        stdout = self.capsys.readouterr().out
        reports = 'xlsx reports found:\n\tsample1.xlsx\n\tsample2.xlsx'

        assert reports in stdout, ('Expected xlsx reports not found')


    def test_cnv_mode_error_raised_when_missing_intervals_bed(self):
        """
        Intervals bed should always be found from CNV calling, check
        we raise an error if this is missing
        """
        # set output of DXManage.find_files() to be empty
        self.mock_find.side_effect = [[], [], []]
        with pytest.raises(
            RuntimeError,
            match=(
                'Failed to find excluded intervals bed file '
                'from job-QaTZ9qEwkEsovKLs14DSdNqb'
            )
        ):
            DXExecute().reports_workflow(
                mode='CNV',
                workflow_id='workflow-GXzvJq84XZB1fJk9fBfG88XJ',
                single_output_dir='/path_to_single/',
                manifest=self.manifest,
                config=self.assay_config['modes']['cnv_reports'],
                start='230925_0943',
                name_patterns=self.assay_config['name_patterns'],
                call_job_id='job-QaTZ9qEwkEsovKLs14DSdNqb'
            )


    def test_cnv_mode_error_raised_when_missing_vcf_files(self):
        """
        Test an error is raised if no VCFs are found
        """
        # patch return of DXManage.find_files to have a bed file but no vcfs
        self.mock_find.side_effect = [
            [],
            [{
                'project': 'project-xxx',
                'id': 'file-xxx'
            }],
            []
        ]

        with pytest.raises(
            RuntimeError,
            match='Failed to find vcfs from job-QaTZ9qEwkEsovKLs14DSdNqb'
        ):
            DXExecute().reports_workflow(
                mode='CNV',
                workflow_id='workflow-GXzvJq84XZB1fJk9fBfG88XJ',
                single_output_dir='/path_to_single/',
                manifest=self.manifest,
                config=self.assay_config['modes']['cnv_reports'],
                start='230925_0943',
                name_patterns={'Gemini': 'X[\d]+'},
                call_job_id='job-QaTZ9qEwkEsovKLs14DSdNqb'
            )


    def test_cnv_mode_exclude_samples_correct(self):
        """
        Test that if exclude_samples is specified, that those samples
        are correctly excluded from the manifest (these will have been
        samples excluded from CNV calling)
        """
        # patch in returned bed and vcfs
        self.mock_find.side_effect = [
            [],
            [{
                'project': 'project-xxx',
                'id': 'file-xxx'
            }],
            [
                {
                    'project': 'project-xxx',
                    'id': 'file-xxx',
                    'describe': {
                        'name': 'X1234_markdup.vcf'
                    }
                },
                {
                    'project': 'project-xxx',
                    'id': 'file-xxx',
                    'describe': {
                        'name': 'X5678_markdup.vcf'
                    }
                }
            ]
        ]

        DXExecute().reports_workflow(
            mode='CNV',
            workflow_id='workflow-GXzvJq84XZB1fJk9fBfG88XJ',
            single_output_dir='/path_to_single/',
            manifest=self.manifest,
            config=self.assay_config['modes']['cnv_reports'],
            start='230925_0943',
            name_patterns=self.assay_config['name_patterns'],
            call_job_id='job-QaTZ9qEwkEsovKLs14DSdNqb',
            exclude=['X1234']
        )

        # check that excluded sample is dumped to stdout, about the best
        # test we can do here since we patch over the manifest later when
        # filtering by files so would be pretty circular to test this
        stdout = self.capsys.readouterr().out

        assert "Excluded 1 samples from manifest: ['X1234']" in stdout, (
            'Exclude sample not correctly excluded'
        )


    def test_snv_mode_error_raised_when_missing_vcfs(self):
        """
        Check correct error is raised if no VCFs are found in given dir
        """
        self.mock_find.return_value = []

        expected_error = (
            "Found no vcf files! SNV reports in /path_to_single/ and subdir "
            "sentieon-dnaseq with pattern .vcf"
        )

        with pytest.raises(RuntimeError, match=expected_error):
            DXExecute().reports_workflow(
                mode='SNV',
                workflow_id='workflow-GXzvJq84XZB1fJk9fBfG88XJ',
                single_output_dir='/path_to_single/',
                manifest=self.manifest,
                config=self.assay_config['modes']['snv_reports'],
                start='230925_0943',
                name_patterns=self.assay_config['name_patterns']
            )


    def test_snv_mode_error_raised_when_missing_mosdepth_files(self):
        """
        Check correct error is raised if no mosdepth files are found in given dir
        """
        # minimal return of find with vcf structure to pass to simulating
        # no mosdepth files
        self.mock_find.side_effect = [
            [],
            [{
                'describe': {
                    'name': 'sample.vcf'
                }
            }],
            []
        ]

        expected_error = (
            "Found no mosdepth files\! SNV reports in \/path_to_single\/ "
            "and subdir eggd_mosdepth with pattern per\-base\.bed\.gz\$\|"
            "reference\.txt\$"
        )

        with pytest.raises(RuntimeError, match=expected_error):
            DXExecute().reports_workflow(
                mode='SNV',
                workflow_id='workflow-GXzvJq84XZB1fJk9fBfG88XJ',
                single_output_dir='/path_to_single/',
                manifest=self.manifest,
                config=self.assay_config['modes']['snv_reports'],
                start='230925_0943',
                name_patterns=self.assay_config['name_patterns']
            )


    def test_snv_mode_filter_manifest_by_files_is_called(self):
        """
        In SNV mode the manifest should be filtered by samples having
        mosdepth files (which also adds the DXObjects to the sample),
        check that this function gets called
        """
        # minimal mock of returned vcf and mosdepth files
        self.mock_find.side_effect = [
            [],
            [{
                'describe': {
                    'name': 'sample.vcf'
                }
            }],
            [
                {
                    'project': 'project-xxx',
                    'id': 'file-xxx',
                    'describe': {
                        'name': 'X1234.per-base.bed.gz'
                    }
                },
                {
                    'project': 'project-xxx',
                    'id': 'file-xxx',
                    'describe': {
                        'name': 'X5678.per-base.bed.gz'
                    }
                }
            ]
        ]

        DXExecute().reports_workflow(
            mode='SNV',
            workflow_id='workflow-GXzvJq84XZB1fJk9fBfG88XJ',
            single_output_dir='/path_to_single/',
            manifest=self.manifest,
            config=self.assay_config['modes']['snv_reports'],
            start='230925_0943',
            name_patterns=self.assay_config['name_patterns']
        )

        # check we called filter_manifest_samples_by_files() for mosdepth
        self.mock_filter_manifest.assert_any_call(
            name='mosdepth',
            files=mock.ANY,
            manifest=mock.ANY,
            pattern=mock.ANY
        )


    def test_samples_no_vcfs_or_mosdepth_files_added_to_errors(self):
        """
        Test if any samples in manifest have no vcfs or mosdepth files
        these get added to the list of returned errors
        """
        self.mock_find.side_effect = [
            [],
            [{
                'describe': {
                    'name': 'sample.vcf'
                }
            }],
            [
                {
                    'project': 'project-xxx',
                    'id': 'file-xxx',
                    'describe': {
                        'name': 'X1234.per-base.bed.gz'
                    }
                },
                {
                    'project': 'project-xxx',
                    'id': 'file-xxx',
                    'describe': {
                        'name': 'X5678.per-base.bed.gz'
                    }
                }
            ]
        ]

        # patch in an error for adding files to the output of
        # filter_manifest_samples_by_file to check it gets returned
        return_copy = deepcopy(self.mock_filter_manifest.return_value)
        return_copy[-2] = ['X1928']
        return_copy[-1] = ['X1928']
        self.mock_filter_manifest.return_value = return_copy

        _, errors, _ = DXExecute().reports_workflow(
            mode='SNV',
            workflow_id='workflow-GXzvJq84XZB1fJk9fBfG88XJ',
            single_output_dir='/path_to_single/',
            manifest=self.manifest,
            config=self.assay_config['modes']['snv_reports'],
            start='230925_0943',
            name_patterns=self.assay_config['name_patterns']
        )

        expected_errors = {
            (
                'Samples in manifest not matching expected Epic pattern '
                '(1) ^[\\d\\w]+-[\\d\\w]+'
            ): ['X1928'],
            'Samples in manifest with no mosdepth files found (1)': ['X1928'],
            'Samples in manifest with no VCF found (1)': ['X1928']
        }

        assert errors == expected_errors, (
            'Expected errors not returned from samples missing files'
        )


    def test_error_raised_if_manifest_empty_after_filtering(self):
        """
        Test that if the manifest is empty after filtering against all
        the different files and patterns that we raise an error since
        there's nothing to launch
        """
        self.mock_find.side_effect = [
            [],
            [{
                'describe': {
                    'name': 'sample.vcf'
                }
            }],
            [
                {
                    'project': 'project-xxx',
                    'id': 'file-xxx',
                    'describe': {
                        'name': 'X1234.per-base.bed.gz'
                    }
                },
                {
                    'project': 'project-xxx',
                    'id': 'file-xxx',
                    'describe': {
                        'name': 'X5678.per-base.bed.gz'
                    }
                }
            ]
        ]

        self.mock_filter_manifest.return_value = [{}, [], []]

        expected_error = "No samples left after filtering to run SNV reports on"

        with pytest.raises(RuntimeError, match=expected_error):
            DXExecute().reports_workflow(
            mode='SNV',
            workflow_id='workflow-GXzvJq84XZB1fJk9fBfG88XJ',
            single_output_dir='/path_to_single/',
            manifest=self.manifest,
            config=self.assay_config['modes']['snv_reports'],
            start='230925_0943',
            name_patterns=self.assay_config['name_patterns']
        )


    def test_name_suffix_integer_incremented(self):
        """
        When setting the output integer suffix for reports, if more than one
        job is being launched for the same sample this suffix should get
        incremented, check this happens
        """
        self.mock_find.side_effect = [
            [],
            [{
                'describe': {
                    'name': 'sample.vcf'
                }
            }],
            [
                {
                    'project': 'project-xxx',
                    'id': 'file-xxx',
                    'describe': {
                        'name': 'X1234.per-base.bed.gz'
                    }
                },
                {
                    'project': 'project-xxx',
                    'id': 'file-xxx',
                    'describe': {
                        'name': 'X5678.per-base.bed.gz'
                    }
                }
            ]
        ]

        self.mock_index.return_value = 1

        # add additional test to manifest for X1234 (just duplicating the
        # one already added)
        filled_manifest = deepcopy(self.mock_filter_manifest.return_value)
        filled_manifest[0]["X1234"]["tests"] = \
            filled_manifest[0]["X1234"]["tests"] * 2
        filled_manifest[0]["X1234"]["indications"] = \
            filled_manifest[0]["X1234"]["indications"] * 2
        filled_manifest[0]["X1234"]["panels"] = \
            filled_manifest[0]["X1234"]["panels"] * 2

        self.mock_filter_manifest.return_value = filled_manifest

        _, _, summary = DXExecute().reports_workflow(
            mode='SNV',
            workflow_id='workflow-GXzvJq84XZB1fJk9fBfG88XJ',
            single_output_dir='/path_to_single/',
            manifest=filled_manifest[0],
            config=self.assay_config['modes']['snv_reports'],
            start='230925_0943',
            name_patterns=self.assay_config['name_patterns']
        )

        assert summary['SNV']['X1234'] == 'X1234_R207.1_SNV_1\nX1234_R207.1_SNV_2', (
            'Suffix for repeat sample incorrect'
        )


    def test_single_gene_reports_name_have_no_colon(self):
        """
        Test that when a single gene report is made that the filename
        does not contain a colon and is replaced with an underscore as
        this breaks file downloads
        """
        self.mock_find.side_effect = [
            [],
            [{
                'describe': {
                    'name': 'sample.vcf'
                }
            }],
            [
                {
                    'project': 'project-xxx',
                    'id': 'file-xxx',
                    'describe': {
                        'name': 'X1234.per-base.bed.gz'
                    }
                },
                {
                    'project': 'project-xxx',
                    'id': 'file-xxx',
                    'describe': {
                        'name': 'X5678.per-base.bed.gz'
                    }
                }
            ]
        ]

        # minimal manifest with parsed in indications and panels, make
        # first sample have single gene test
        filled_manifest = deepcopy(self.mock_filter_manifest.return_value)
        filled_manifest[0]["X1234"]["tests"] = [["_HGNC:1234"]]

        self.mock_filter_manifest.return_value = filled_manifest
        self.mock_index.return_value = 1

        _, _, summary = DXExecute().reports_workflow(
            mode='SNV',
            workflow_id='workflow-GXzvJq84XZB1fJk9fBfG88XJ',
            single_output_dir='/path_to_single/',
            manifest=filled_manifest[0],
            config=self.assay_config['modes']['snv_reports'],
            start='230925_0943',
            name_patterns=self.assay_config['name_patterns']
        )

        assert summary['SNV']['X1234'] == 'X1234_HGNC_1234_SNV_1', (
            'naming of single gene test incorrect'
        )


    def test_sample_limit_works(self):
        """
        Test when sample limit is set that it works as expected
        """
        self.mock_find.side_effect = [
            [],
            [{
                'describe': {
                    'name': 'sample.vcf'
                }
            }],
            [
                {
                    'project': 'project-xxx',
                    'id': 'file-xxx',
                    'describe': {
                        'name': 'X1234.per-base.bed.gz'
                    }
                },
                {
                    'project': 'project-xxx',
                    'id': 'file-xxx',
                    'describe': {
                        'name': 'X5678.per-base.bed.gz'
                    }
                }
            ]
        ]

        DXExecute().reports_workflow(
            mode='SNV',
            workflow_id='workflow-GXzvJq84XZB1fJk9fBfG88XJ',
            single_output_dir='/path_to_single/',
            manifest=self.manifest,
            config=self.assay_config['modes']['snv_reports'],
            start='230925_0943',
            name_patterns=self.assay_config['name_patterns'],
            sample_limit=1
        )

        stdout = self.capsys.readouterr().out

        assert "Sample limit hit, stopping launching further jobs" in stdout, (
            "Sample limit param didn't break as expected"
        )


class TestDXExecuteArtemis(unittest.TestCase):
    """
    Test for DXExecute.artemis

    This is a reasonably pointless test since we're going to patch all
    the function calls it makes, which is all this function does, but also
    I want the sweet dopamine hit of it going green in the test report so
    here it is ¯\_(ツ)_/¯
    """

    @patch('utils.dx_requests.make_path')
    @patch('utils.dx_requests.dxpy.describe')
    @patch('utils.dx_requests.dxpy.DXApp')
    def test_called(
        self,
        mock_app,
        mock_describe,
        mock_path
    ):
        """
        Test when we run artemis that we get back the job ID which is stored
        as '_dxid' attribute of the DXJob dx object
        """
        # patch in a DXJob object to the return of calling DXApp.run()
        job_obj = dxpy.DXJob('job-QaTZ9qEwkEsovKLs14DSdNqb')
        job_obj._dxid = 'job-QaTZ9qEwkEsovKLs14DSdNqb'
        mock_app.return_value.run.return_value = job_obj

        mock_describe.return_value = {
            'name': 'eggd_artemis',
            'version': '1.3.0'
        }

        job = DXExecute().artemis(
            single_output_dir='/output_path/',
            app_id='app-xxx',
            dependent_jobs=[],
            start='230922_1012',
            qc_xlsx='file-xxx',
            capture_bed='file-xxx',
            snv_output=None,
            cnv_output=None,
            url_duration=None
        )

        assert job == 'job-QaTZ9qEwkEsovKLs14DSdNqb', (
            'Job ID returned from running Artemis incorrect'
        )

    @patch('utils.dx_requests.make_path')
    @patch('utils.dx_requests.dxpy.describe')
    @patch('utils.dx_requests.dxpy.DXApp')
    def test_multiqc_report_added(
        self,
        mock_app,
        mock_describe,
        mock_path
    ):
        """
        Test that when eggd_artemis >=1.4.0 that multiqc_report is
        specified as an input when running the app
        """
        # mock app describe output to be 1.4.0 => add multiqc_report
        mock_describe.return_value = {
            'name': 'eggd_artemis',
            'version': '1.4.0'
        }

        DXExecute().artemis(
            single_output_dir='/output_path/',
            app_id='app-xxx',
            dependent_jobs=[],
            start='230922_1012',
            qc_xlsx='file-xxx',
            capture_bed='file-xxx',
            snv_output=None,
            cnv_output=None,
            url_duration=None,
            multiqc_report='file-xxx'
        )

        run_input = mock_app.return_value.run.call_args.kwargs

        self.assertEqual(run_input['app_input']['multiqc_report'], 'file-xxx')

    @patch('utils.dx_requests.make_path')
    @patch('utils.dx_requests.dxpy.describe')
    @patch('utils.dx_requests.dxpy.DXApp')
    def test_additional_inputs_passed_if_exist(
        self,
        mock_app,
        mock_describe,
        mock_path
    ):
        """
        Test that if additional inputs are passed to the function that
        these are added to the app input when running the app
        """
        # mock app describe output to be 1.4.0 => add multiqc_report
        mock_describe.return_value = {
            'name': 'eggd_artemis',
            'version': '1.4.0'
        }

        additional_inputs = {
            "input_1": "value_1",
            "input_2": 12
        }

        DXExecute().artemis(
            single_output_dir='/output_path/',
            app_id='app-xxx',
            dependent_jobs=[],
            start='230922_1012',
            qc_xlsx='file-xxx',
            snv_output=None,
            cnv_output=None,
            **additional_inputs
        )

        run_input = mock_app.return_value.run.call_args.kwargs
        self.assertEqual(run_input['app_input']['input_1'], "value_1")
        self.assertEqual(run_input['app_input']['input_2'], 12)

    @patch('utils.dx_requests.make_path')
    @patch('utils.dx_requests.dxpy.describe')
    @patch('utils.dx_requests.dxpy.DXApp')
    def test_additional_inputs_not_passed_if_exist(
        self,
        mock_app,
        mock_describe,
        mock_path
    ):
        """
        Test that if no additional inputs exist they are not passed to
        to the app input when running the app
        """
        # mock app describe output to be 1.4.0 => add multiqc_report
        mock_describe.return_value = {
            'name': 'eggd_artemis',
            'version': '1.4.0'
        }

        additional_inputs = {}

        DXExecute().artemis(
            single_output_dir='/output_path/',
            app_id='app-xxx',
            dependent_jobs=[],
            start='230922_1012',
            qc_xlsx='file-xxx',
            snv_output=None,
            cnv_output=None,
            **additional_inputs
        )

        expected_run_inputs = {
            'snv_path', 'cnv_path', 'qc_status'
        }

        kwargs = mock_app.return_value.run.call_args.kwargs
        run_input = kwargs['app_input']
        self.assertEqual(set(run_input.keys()), expected_run_inputs)

class TestDXExecuteTerminate(unittest.TestCase):
    """
    Unit tests for DXExecute.terminate

    Function takes in a list of jobs / analysis IDs and calls relevant
    dxpy call to terminate it, used when running testing to stop all
    launched jobs
    """
    def setUp(self):
        """Setup up mocks of terminate"""
        self.job_patch = mock.patch('utils.dx_requests.dxpy.DXJob')
        self.job_terminate_patch = mock.patch(
            'utils.dx_requests.dxpy.bindings.DXJob.terminate')
        self.analysis_patch = mock.patch(
            'utils.dx_requests.dxpy.DXAnalysis'
        )
        self.analysis_terminate_patch = mock.patch(
            'utils.dx_requests.dxpy.bindings.DXAnalysis.terminate')

        self.mock_job = self.job_patch.start()
        self.mock_job_terminate = self.job_terminate_patch.start()
        self.mock_analysis = self.analysis_patch.start()
        self.mock_analysis_terminate = self.analysis_terminate_patch.start()


    def tearDown(self):
        self.mock_job.stop()
        self.mock_job_terminate.stop()
        self.mock_analysis.stop()
        self.mock_analysis_terminate.stop()


    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        """Capture stdout to provide it to tests"""
        self.capsys = capsys


    def test_jobs_terminate(self):
        """
        Test when jobs provided they get terminate() called
        """
        # patch job object on which terminate() will get called
        self.mock_job.return_value = dxpy.bindings.DXJob(dxid='localjob-')

        DXExecute().terminate(['job-xxx', 'job-yyy'])

        self.mock_job_terminate.assert_called()


    def test_analysis_terminate(self):
        """
        Test when analysis IDs provided they get terminate() called
        """
        # patch job object on which terminate() will get called
        self.mock_analysis.return_value = dxpy.bindings.DXAnalysis(
            dxid='analysis-QaTZ9qEwkEsovKLs14DSdNqb')

        DXExecute().terminate(['analysis-xxx', 'analysis-yyy'])

        self.mock_analysis_terminate.assert_called()


    def test_errors_caught(self):
        """
        Test if any errors are raised these are caught
        """
        # patch the call to DXJob.terminate() to raise an Exception
        self.mock_job.return_value = dxpy.bindings.DXJob(
            dxid='job-QaTZ9qEwkEsovKLs14DSdNqb')
        self.mock_job_terminate.side_effect = Exception('oh no :sadpepe:')

        DXExecute().terminate(['job-xxx'])

        stdout = self.capsys.readouterr().out

        assert 'Error terminating job job-xxx: oh no :sadpepe:' in stdout, (
            'Error in terminating job not correctly caught'
        )
