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
        Test when an invalid assay string passed to -iassay error is raised
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
        Test when assay config dir specified is empty that error is raised
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
        Test when no files are found in single output dir error is raised
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

    def test_error_raised_for_cnv_reports_invalid(self, mocker):
        """
        Test when CNV reports is to be run that errors is raised if
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
