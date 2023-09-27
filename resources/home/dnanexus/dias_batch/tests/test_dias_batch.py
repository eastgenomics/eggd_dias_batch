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
