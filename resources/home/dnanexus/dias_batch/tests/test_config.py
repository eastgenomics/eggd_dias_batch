"""
Tests for functions in utils/config.py
"""
from copy import deepcopy
import json
import os
import sys
import unittest

import pytest


sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))

from utils import config


TEST_DATA_DIR = (
    os.path.join(os.path.dirname(__file__), 'test_data')
)


class TestFillConfigReferenceInputs():
    """
    Tests for config.fill_config_reference_inputs()

    Function takes the reference files defined in the top section of
    the config file, and parses these into the inputs sections where
    they are defined as INPUT-{reference_name} to then run jobs
    """
    # read our test config file in like would be read from dx file
    with open(f"{TEST_DATA_DIR}/example_config.json") as file_handle:
        example_config = json.load(file_handle)

    # read in populated config for comparing
    with open(f"{TEST_DATA_DIR}/example_filled_config.json") as file_handle:
        filled_config = json.load(file_handle)


    def test_all_reference_files_added(self):
        """
        Test that all reference files in the 'reference_files' section of
        the config file are correctly parsed in when all provided in
        config file as project-xxx:file-xxx
        """
        parsed_config = config.fill_config_reference_inputs(self.example_config)

        assert parsed_config == self.filled_config, (
            "Reference files incorrectly parsed into config"
        )

    def test_reference_added_as_just_file_id(self):
        """
        Test when reference provided as just file-xxx (i.e. not project and
        file ID) it is added correctly
        """
        config_copy = deepcopy(self.example_config)
        config_copy['reference_files']['genepanels'] = "file-GVx0vkQ433Gvq63k1Kj4Y562"

        parsed_config = config.fill_config_reference_inputs(config_copy)

        correct_format = {"$dnanexus_link": "file-GVx0vkQ433Gvq63k1Kj4Y562"}
        filled_input = parsed_config['modes']['workflow_1'][
            'inputs']['stage_2.input_1']

        assert correct_format == filled_input, (
            'Reference incorrectly added to config when provided as file ID'
        )

    def test_reference_added_as_dx_link_mapping(self):
        """
        Test when reference provided as dx_link mapping it is added correctly
        """
        config_copy = deepcopy(self.example_config)
        config_copy['reference_files']['genepanels'] = {
            "$dnanexus_link": {
              "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
              "id": "file-GVx0vkQ433Gvq63k1Kj4Y562"
            }
        }

        parsed_config = config.fill_config_reference_inputs(config_copy)

        assert parsed_config['modes'] == self.filled_config['modes'], (
            'Reference incorrectly added to config when provided as dx link'
        )

    def test_malformed_reference(self):
        """
        Test when config file has an input that contains invalid reference
        file that this raises a RuntimeError
        """
        config_copy = deepcopy(self.example_config)
        config_copy['reference_files']['genepanels'] = 'INPUT-invalid'

        with pytest.raises(RuntimeError):
            config.fill_config_reference_inputs(config_copy)

    def test_app_no_inputs(self, capsys):
        """
        Test when an app/workflow in the config has no inputs dict defined
        that we print a warning and continue
        """
        config_copy = deepcopy(self.example_config)
        config_copy['modes']['app1'] = {}

        config.fill_config_reference_inputs(config_copy)
        stdout = capsys.readouterr().out

        correct_print = (
            "WARNING: app1 in the config does not appear to "
            "have an 'inputs' key, skipping adding reference files"
        )

        assert correct_print in stdout, (
            'App missing inputs did not print expected warning'
        )


class TestAddDynamicInputs(unittest.TestCase):
    """
    Test for config.add_dynamic_inputs()

    Function parses through input dict to replace 'INPUT-xxx' strings with
    corresponding string inputs (such as panel strings and clinical
    indication).

    Currently implemented patterns to replace include:
        - INPUT-clinical_indications
        - INPUT-test_codes
        - INPUT-panels
        - INPUT-sample_name
    """
    def test_all_supported_placeholders_replaced(self):
        """
        Test that the supported placeholder text is correctly parsed in
        """
        example_config = {
            "stage-xxx.indication": "INPUT-clinical_indications",
            "stage-xxx.panel": "INPUT-panels",
            "stage-yyy.test": "INPUT-test_codes",
            "stage-yyy.name": "INPUT-sample_name",
            "stage-yyy.limit": 1,
            "stage-zzz.bed": {
                "$dnanexus_link": {
                        "project": "project-xxx",
                        "id": "file-xxx"
                    }
            }
        }

        filled_config = config.add_dynamic_inputs(
            config=example_config,
            clinical_indications='R1.1_foo_bar',
            test_codes='R1.1',
            panels='panel1',
            sample_name='sample1'
        )

        correct_config = {
            'stage-xxx.indication': 'R1.1_foo_bar',
            'stage-xxx.panel': 'panel1',
            'stage-yyy.test': 'R1.1',
            'stage-yyy.name': 'sample1',
            "stage-yyy.limit": 1,
            "stage-zzz.bed": {
                "$dnanexus_link": {
                        "project": "project-xxx",
                        "id": "file-xxx"
                    }
            }
        }

        self.assertEqual(filled_config, correct_config)

    def test_invalid_placeholder_raises_assetion_error(self):
        """
        Test that when an invalid INPUT- placeholder in config that
        we correctly raise an AssertionError
        """
        example_config = {
            "stage-xxx.indication": "INPUT-blarg"
        }

        with pytest.raises(AssertionError):
            config.add_dynamic_inputs(
                config=example_config,
                clinical_indications='R1.1_foo_bar',
                test_codes='R1.1',
                panels='panel1',
                sample_name='sample1'
            )
