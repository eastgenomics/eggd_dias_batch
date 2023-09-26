"""
Limited test suite for dx_requests.py

The majority of functions in dx_requests.py relate to interacting with
DNAnexus via dxpy API calls to either manage data (in DXManage) or for
launching jobs (in DXExecute), therefore a lot of this is a pain to write
unit tests for. The majority of the logic of parsing the output of functions
in DXManage is in utils.py with its own unit tests.
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
            {'assay': 'test', 'version': '2.0.0'},
            {'assay': 'test', 'version': '1.1.11'}
        ]

        config = DXManage().get_assay_config(
            path='project-xxx:/test_path',
            assay='test'
        )

        assert config['version'] == '2.0.0', (
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
