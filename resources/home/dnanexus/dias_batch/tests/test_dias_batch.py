"""
Tests for CheckInputs() that are run at the beginning of dias batch
"""
from copy import deepcopy
import json
import os
import pytest
import re
import subprocess
import sys
import unittest
from unittest.mock import patch

import dxpy
import pandas as pd


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

    def test_check_single_output_dir()
