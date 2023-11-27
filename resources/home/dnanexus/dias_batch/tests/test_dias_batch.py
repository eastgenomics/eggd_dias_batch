"""
Tests for CheckInputs() that are run at the beginning of dias batch
"""
import os
import sys
from unittest.mock import patch


sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))

from ..dias_batch import CheckInputs


TEST_DATA_DIR = (
    os.path.join(os.path.dirname(__file__), 'test_data')
)

class TestCheckInputs():
    """
    Tests for each individual run time check performed on app inputs.

    Each test will mimic setup performed in CheckInputs.__init__ with the
    minimum inputs required set for that check, but not initialise the class
    to stop it running all checks for every test
    """
    def test_invalid_assay_str_error_raised(self, mocker):
        """
        Test error is raised when an invalid assay string passed to -iassay
        """
        mocker.patch.object(CheckInputs, "__init__", return_value=None)
        check = CheckInputs()
        check.errors = []
        check.inputs = {
            'assay': 'invalidAssay'
        }

        check.check_assay()

        assert check.errors == ['Invalid assay passed: invalidAssay'], (
            'Error not raised for invalid assay string'
        )

    @patch('utils.dx_requests.dxpy.find_data_objects')
    def test_assay_config_dir(self, test_patch, mocker):
        """
        Test that error is raised when assay config dir specified is empty
        """
        test_patch.return_value = []

        mocker.patch.object(CheckInputs, "__init__", return_value=None)
        mocker.return_value = None
        check = CheckInputs()
        check.errors = []
        check.inputs = {
            'assay_config_dir': 'project-xxx:/some_empty_path'
        }

        check.check_assay_config_dir()

        correct_error = [
            'Given assay config dir appears to contain no config files: '
            'project-xxx:/some_empty_path'
        ]

        assert check.errors == correct_error, (
            'Correct error not raised for empty assay config dir'
        )

    @patch('utils.dx_requests.dxpy.find_data_objects')
    def test_check_single_output_dir(self, test_patch, mocker):
        """
        Test that error is raised when no files are found in single output dir
        """
        test_patch.return_value = []

        mocker.patch.object(CheckInputs, "__init__", return_value=None)
        mocker.return_value = None
        check = CheckInputs()
        check.errors = []
        check.inputs = {
            'single_output_dir': 'project-xxx:/some_empty_path'
        }

        check.check_single_output_dir()

        correct_error = [
            'Given Dias single output dir appears to be empty: '
            'project-xxx:/some_empty_path'
        ]

        assert check.errors == correct_error, (
            'Error not raised for empty single directory'
        )

    def test_check_no_mode_set(self, mocker):
        """
        Check correct error raised if no mode set
        """
        mocker.patch.object(CheckInputs, "__init__", return_value=None)
        mocker.return_value = None
        check = CheckInputs()
        check.errors = []
        check.inputs = {}

        check.check_mode_set()

        assert check.errors == ['No mode specified to run in'], (
            'Error not raised for no running mode set'
        )

    def test_error_raised_for_no_manifest_with_reports_mode(self, mocker):
        """
        Test error is raised when a reports mode is set and no manifest given
        """
        mocker.patch.object(CheckInputs, "__init__", return_value=None)
        mocker.return_value = None
        check = CheckInputs()
        check.errors = []

        for mode in [
            'cnv_call', 'cnv_reports', 'snv_reports', 'mosaic_reports'
        ]:
            check.inputs = {
                mode: True
            }

            check.check_mode_set()

        correct_error = ['Reports argument specified with no manifest file'] * 3

        assert check.errors == correct_error, (
            'Error not raised for reports mode and missing manifest'
        )

    def test_error_raised_invalid_cnv_mode(self, mocker):
        """
        Test error is raised when both cnv_call and cnv_call_job_id specified
        """
        mocker.patch.object(CheckInputs, "__init__", return_value=None)
        mocker.return_value = None
        check = CheckInputs()
        check.errors = []
        check.inputs = {}

        check.inputs['cnv_call'] = True
        check.inputs['cnv_call_job_id'] = 'job-xxx'

        check.check_cnv_call_and_cnv_call_job_id_mutually_exclusive()

        correct_error = ([
            "Both mutually exclusive cnv_call and "
            "cnv_call_job_id inputs specified"
        ])

        assert check.errors == correct_error, (
            'Incorrect error raised for checking cnv call mode'
        )


    def test_error_raised_for_cnv_reports_invalid(self, mocker):
        """
        Test when CNV reports is to be run that an error is raised if
        CNV call mode or CNV call job ID is missing
        """
        mocker.patch.object(CheckInputs, "__init__", return_value=None)
        mocker.return_value = None
        check = CheckInputs()
        check.errors = []
        check.inputs = {'cnv_reports': True}

        check.check_cnv_calling_for_cnv_reports()

        correct_error = [
            "Running CNV reports without first running CNV calling and "
            "cnv_call_job_ID not specified. Please rerun with "
            "'-icnv_call=true or specify a job ID with '-icnv_call_job_id'"
        ]

        assert check.errors == correct_error, (
            "Error not raised for CNV reports missing CNV call / job ID"
        )


    def test_exclude_samples_with_file_id(self, mocker):
        """
        Test for check_exclude_samples_file_id() to check if a file ID
        has been provided to exclude_samples instead of exclude_samples_file
        """
        mocker.patch.object(CheckInputs, "__init__", return_value=None)
        mocker.return_value = None
        check = CheckInputs()
        check.errors = []

        check.inputs = {
            "exclude_samples": "file-abc123"
        }

        check.check_exclude_samples_file_id()

        correct_error = ([
            "DNAnexus file ID provided to -iexclude_samples, "
            "rerun and provide this as -iexclude_samples_file=file-abc123"
        ])

        assert check.errors == correct_error, (
            "Error not raise from file ID provided to exclude_samples"
        )


class TestMain():
    """
    Tests for dias_batch.main

    This is the main entry point into the app, tests are just to show that
    functions are called when expected as there is little other logic in here
    """
    # TODO I should probably finish this test off at some point
    pass
