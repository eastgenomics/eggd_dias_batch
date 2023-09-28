"""
Tests for general functions in utils.py

The following functions are not covered as they are either
not needed to be tested (i.e. prettier_print()) or are a
pain to write tests for (i.e. write_summary_report()):

- time_stamp()
- prettier_print()
- write_summary_report()
"""
from copy import deepcopy
import json
import os
import pytest
import re
import subprocess
import sys

import pandas as pd


sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))

from utils import utils


TEST_DATA_DIR = (
    os.path.join(os.path.dirname(__file__), 'test_data')
)


class TestCheckReportIndex():
    """
    Tests for utils.check_report_index()

    Function takes a list of files returned from DXManage.find_files()
    and a sample name stem, filters down to just xlsx reports for the
    given sample and then gets the highest index found from the int before
    the '.xlsx' suffix
    """
    reports = [
        "X223420-GM2225190_SNV_1.xlsx",
        "X223420-GM2225190_SNV_2.xlsx",
        "X223420-GM2225190_CNV_1.xlsx"
    ]

    def test_max_suffix_snv(self):
        """
        Test that max suffix returned for SNV is correct
        """
        suffix = utils.check_report_index(
            name="X223420-GM2225190_SNV",
            reports=self.reports
        )

        assert suffix == 3, (
            "Wrong suffix returned for sample with 2 previous reports"
        )

    def test_max_suffix_cnv(self):
        """
        Test max suffix returned for CNV is correct
        """
        suffix = utils.check_report_index(
            name="X223420-GM2225190_CNV",
            reports=self.reports
        )

        assert suffix == 2, (
            "Wrong suffix returned for sample with 1 previous report"
        )

    def test_suffix_1_returned_no_previous_reports(self):
        """
        Test suffix '1' returned when sample has no previous reports
        """
        suffix = utils.check_report_index(
            name="sample_no_previous_reports",
            reports=self.reports
        )

        assert suffix == 1, (
            "Wrong suffix returned for sample with no previous reports"
        )

    def test_prev_samples_but_no_suffix_in_name(self):
        """
        Test when a previous report found for same samplename stem, but
        .xlsx file doesn't appear to have an integer suffix
        """
        previous_reports = [
            "X223420-GM2225190_SNV.xlsx",
            "X223420-GM2225190_SNV.xlsx",
        ]

        suffix = utils.check_report_index(
            name="X223420-GM2225190_SNV",
            reports=previous_reports
        )

        assert suffix == 1, (
            "Wrong suffix returned for sample with no previous report suffix"
        )


class TestMakePath():
    """
    Tests for utils.make_path()
    
    Function expects  to take any number of strings with variable '/' and
    format nicely as a path for DNAnexus queries, dropping project-
    prefix if present
    """
    def test_path_mix(self):
        """
        Test mix of strings builds path correctly
        """
        path_parts = [
            "project-abc123:/dir1"
            "/double_slash/",
            "/prefix_slash",
            "suffix_slash/",
            "no_slash",
            "and_another/",
            "/much_path",
            "many_lines/"
        ]

        path = utils.make_path(*path_parts)

        correct_path = (
            "/dir1/double_slash/prefix_slash/suffix_slash/no_slash/"
            "and_another/much_path/many_lines/"
        )

        assert path == correct_path, "Invalid path built"


class TestFillConfigReferenceInputs():
    """
    Tests for utils.fill_config_reference_inputs()

    Function takes the reference files defined in the top section of
    the config file, and parses these into the inputs sections where
    they are defined as INPUT-{reference_name} to then run jobs
    """
    # read our test config file in like would be read from dx file
    with open(f"{TEST_DATA_DIR}/example_config.json") as file_handle:
        config = json.load(file_handle)

    # read in populated config for comparing
    with open(f"{TEST_DATA_DIR}/example_filled_config.json") as file_handle:
        filled_config = json.load(file_handle)


    def test_all_reference_files_added(self):
        """
        Test that all reference files in the 'reference_files' section of
        the config file are correctly parsed in when all provided in
        config file as project-xxx:file-xxx
        """
        parsed_config = utils.fill_config_reference_inputs(self.config)

        assert parsed_config == self.filled_config, (
            "Reference files incorrectly parsed into config"
        )

    def test_reference_added_as_just_file_id(self):
        """
        Test when reference provided as just file-xxx (i.e. not project and
        file ID) it is added correctly
        """
        config_copy = deepcopy(self.config)
        config_copy['reference_files']['genepanels'] = "file-GVx0vkQ433Gvq63k1Kj4Y562"

        parsed_config = utils.fill_config_reference_inputs(config_copy)

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
        config_copy = deepcopy(self.config)
        config_copy['reference_files']['genepanels'] = {
            "$dnanexus_link": {
              "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
              "id": "file-GVx0vkQ433Gvq63k1Kj4Y562"
            }
        }

        parsed_config = utils.fill_config_reference_inputs(config_copy)

        assert parsed_config['modes'] == self.filled_config['modes'], (
            'Reference incorrectly added to config when provided as dx link'
        )

    def test_malformed_reference(self):
        """
        Test when config file has an input that contains invalid reference
        file that this raises a RuntimeError
        """
        config_copy = deepcopy(self.config)
        config_copy['reference_files']['genepanels'] = 'INPUT-invalid'

        with pytest.raises(RuntimeError):
            utils.fill_config_reference_inputs(config_copy)


class TestParseGenePanels():
    """
    Tests for utils.parse_genepanels() that reads in the genepanels file,
    drops the HGNC ID column and keeps the unique rows left (i.e. one row
    per clinical indication / panel), and adds the test code as a separate
    column.
    """
    with open(f"{TEST_DATA_DIR}/genepanels.tsv") as file_handle:
        # parse genepanels file like is done in dias_batch.main()
        genepanels_data = file_handle.read().splitlines()
        genepanels_df = utils.parse_genepanels(genepanels_data)

    def test_correct_indications(self):
        """
        Check that all the correct unique clinical indications parsed
        from the file
        """
        output = subprocess.run(
            f"cut -f1 {TEST_DATA_DIR}/genepanels.tsv | sort | uniq",
            shell=True, capture_output=True, check=True
        )

        stdout = sorted([x for x in output.stdout.decode().split('\n') if x])

        correct_indications = sorted(set(
            self.genepanels_df['indication'].tolist()))

        assert stdout == correct_indications, (
            "Incorrect indications parsed from genepanels file"
        )

    def test_correct_panels(self):
        """
        Check that all the correct unique panels parsed from the file
        """
        output = subprocess.run(
            f"cut -f2 {TEST_DATA_DIR}/genepanels.tsv | sort | uniq",
            shell=True, capture_output=True, check=True
        )

        stdout = sorted([x for x in output.stdout.decode().split('\n') if x])

        correct_panels = sorted(set(
            self.genepanels_df['panel_name'].tolist()))

        assert stdout == correct_panels, (
            "Incorrect panel names parsed from genepanels file"
        )


class TestSplitGenePanelsTestCodes():
    """
    Tests for utils.split_gene_panels_test_codes()
    
    Function takes the read in genepanels file and splits out the test code
    that prefixes the clinical indication (i.e. R337.1 -> R337.1_CADASIL_G)
    """
    # read in genepanels file in the same manner as utils.parse_genepanels()
    # up to the point of calling split_gene_panels_test_codes()
    with open(f"{TEST_DATA_DIR}/genepanels.tsv") as file_handle:
        # parse genepanels file like is done in dias_batch.main()
        genepanels_data = file_handle.read().splitlines()
        genepanels = pd.DataFrame(
            [x.split('\t') for x in genepanels_data],
            columns=['indication', 'panel_name', 'hgnc_id']
        )
        genepanels.drop(columns=['hgnc_id'], inplace=True)  # chuck away HGNC ID
        genepanels.drop_duplicates(keep='first', inplace=True)
        genepanels.reset_index(inplace=True)


    def test_genepanels_unchanged_by_splitting(self):
        """
        Test that no rows get added or removed
        """
        panel_df = utils.split_genepanels_test_codes(self.genepanels)

        current_indications = self.genepanels['indication'].tolist()
        split_indications = panel_df['indication'].tolist()

        assert current_indications == split_indications, (
            'genepanels indications changed when splitting test codes'
        )

    def test_splitting_r_code(self):
        """
        Test splitting of R code from a clinical indication works
        """
        panel_df = utils.split_genepanels_test_codes(self.genepanels)
        r337_code = panel_df[panel_df['indication'] == 'R337.1_CADASIL_G']

        assert r337_code['test_code'].tolist() == ['R337.1'], (
            "Incorrect R test code parsed from clinical indication"
        )

    def test_splitting_c_code(self):
        """
        Test splitting of C code from a clinical indication works
        """
        panel_df = utils.split_genepanels_test_codes(self.genepanels)
        c1_code = panel_df[panel_df['indication'] == 'C1.1_Inherited Stroke']

        assert c1_code['test_code'].tolist() == ['C1.1'], (
            "Incorrect C test code parsed from clinical indication"
        )

    def test_catch_multiple_indication_for_one_test_code(self):
        """
        We have a check that if a test code links to more than one clinical
        indication (which it shouldn't), we can add in a duplicate and test
        that this gets caught
        """
        genepanels_copy = deepcopy(self.genepanels)
        genepanels_copy = pd.concat([genepanels_copy,
            pd.DataFrame([{
                'test_code': 'R337.1',
                'indication': 'R337.1_CADASIL_G_COPY',
                'panel_name': 'R337.1_CADASIL_G_COPY'
            }])
        ])

        with pytest.raises(RuntimeError):
            utils.split_genepanels_test_codes(genepanels_copy)


class TestParseManifest:
    """
    Tests for utils.parse_manifest()

    Expects to take in a list of lines read from manifest file in DNAnexus
    by DXManage().read_dxfile(), and parses this into a dict mapping
    SampleID -> list of list of TestCodes to generate reports for
    """
    # read in Gemini manifest
    with open(os.path.join(TEST_DATA_DIR, 'gemini_manifest.tsv')) as file_handle:
        gemini_data = file_handle.read().splitlines()

    # read in Epic manifest
    with open(os.path.join(TEST_DATA_DIR, 'epic_manifest.txt')) as file_handle:
        epic_data = file_handle.read().splitlines()

    def test_gemini_not_two_columns(self):
        """
        Test when tab separated file provided it only has 2 columns. This
        is expected to be a manifest from Gemini.
        """
        data = deepcopy(self.gemini_data)
        data.append('a\tb\tc')

        with pytest.raises(AssertionError):
            utils.parse_manifest(data)


    def test_gemini_multiple_lines_combined(self):
        """
        Test when multiple test codes for one sample provided on separate
        lines that these get combined into one dict entry.

        X223441	_HGNC:795
        X223441	_HGNC:16627
        X223441 R228.1_Tuberous sclerosis_G
                    |
                    ▼
        {'X223441': [['_HGNC:795', '_HGNC:16627', 'R228.1']]}
        """
        manifest, _ = utils.parse_manifest(self.gemini_data)

        correct_tests = [['_HGNC:795', '_HGNC:16627', 'R228.1']]

        assert manifest['X223441']['tests'] == correct_tests, (
            "Multiple tests in Gemini manifest not correctly merged"
        )


    def test_epic_correct_number_lines_parsed(self):
        """
        Test when reading Epic manifest we get the correct number of
        lines since we drop first row (batch ID) and second (column names)
        """
        manifest, _  = utils.parse_manifest(self.epic_data)

        assert len(manifest.keys()) == 5, (
            'Incorrect number of lines parsed from Epic manifest'
        )


    def test_epic_required_columns_checked(self):
        """
        parse_manifest() has an assert to check for required
        columns when parsing Epic manifest, check that all of these
        are correctly picked up
        """
        columns = [
            'Instrument ID', 'Specimen ID', 'Re-analysis Instrument ID',
            'Re-analysis Specimen ID', 'Test Codes'
        ]

        for column in columns:
            # copy Epic data and drop out required column to ensure
            # error is raised
            data = deepcopy(self.epic_data)
            data[1] = re.sub(rf"{column}", 'NA', data[1])

            with pytest.raises(AssertionError):
                utils.parse_manifest(data)


    def test_epic_spaces_and_sp_prefix_removed(self):
        """
        When parsing Epic manifest spaces should be stripped
        from any required columns and SP- IDs from specimen
        columns in case these get accidentally included
        """
        # add in some spaces to the last row of the data
        data = deepcopy(self.epic_data)
        data[-1] = ';'.join(
            [f"{x[:5]} {x[5:]}" if x else x for x in data[-1].split(';')]
        )

        # add SP- prefix to specimen columns
        data[-1] = ';'.join([
            f"SP-{x}" if idx in (0, 2) else x for idx, x
            in enumerate(data[-1].split(';'))
        ])

        # parse manifest => should remove our mess from above
        manifest, _ = utils.parse_manifest(data)

        errors = []

        if any([' ' in x for x in manifest.keys()]):
            errors.append('Spaces in sample ID')

        if any(['SP' in x for x in manifest.keys()]):
            errors.append('SP in Sample ID')

        assert not errors, errors


    def test_epic_reanalysis_ids_used(self):
        """
        Where 'Re-analysis Specimen ID' or 'Re-analysis Instrument ID'
        are specified these should be used over the Specimen ID and
        Instrument columns, as these contain the original IDs that we need
        
        The last row in our test epic manifest has GM2308111 and X225111
        for the reanalysis IDs, therefore we check this ends up in our
        manifest dict
        """
        manifest, _ = utils.parse_manifest(self.epic_data)
        assert 'X225111-GM2308111' in manifest.keys(), (
            'Reanalysis IDs not correctly parsed into manifest'
        )


    def test_epic_missing_sample_id_caught(self):
        """
        Reanalysis ID and SampleID columns are concatenations of
        {Re-analysis Specimen ID}-{Re-analysis Instrument ID} and 
        {Specimen ID}-{Instrument ID} columns, respectively.
        
        We check generated sample IDs are valid against
        r'[\d\w]+-[\d\w]+', therefore test we catch malformed IDs 
        """
        data = deepcopy(self.epic_data)

        # drop specimen ID and reanalysis specimen ID
        # row 2 => first row of sample data w/ normal specimen - instrument ID
        data[2] = ';'.join([
            '' if idx == 2 else x for idx, x in enumerate(data[2].split(';'))
        ])

        # last row data => sample w/ reanalysis fields
        data[-1] = ';'.join([
            '' if idx == 0 else x for idx, x in enumerate(data[-1].split(';'))
        ])

        with pytest.raises(RuntimeError):
            utils.parse_manifest(data)


    def test_invalid_manifest(self):
        """
        Manifest file passed is checked if every row contains '\t' =>
        from Gemini or rows 3: contain ';' => from Epic. Test we correctly
        raise an error on something else being passed
        """
        # simulate simple csv file contents
        data = [
            'header1,header2', 'data1,data2', 'data3,data4'
        ]

        with pytest.raises(RuntimeError):
            utils.parse_manifest(data)


class TestFilterManifestSamplesByFiles():
    """
    TODO
    """


class TestCheckManifestValidTestCodes():
    """
    Tests for utils.check_manifest_valid_test_codes()

    Function parses through all test codes from the manifest, and checks
    they are valid against what we have in genepanels. If any are invalid
    for any sample, an error is raised.
    """
    with open(os.path.join(TEST_DATA_DIR, 'epic_manifest.txt')) as file_handle:
        epic_data = file_handle.read().splitlines()
        manifest, _ = utils.parse_manifest(epic_data)

    # read in genepanels file in the same manner as utils.parse_genepanels()
    # up to the point of calling split_gene_panels_test_codes()
    with open(f"{TEST_DATA_DIR}/genepanels.tsv") as file_handle:
        # parse genepanels file like is done in dias_batch.main()
        genepanels_data = file_handle.read().splitlines()
        genepanels = utils.parse_genepanels(genepanels_data)


    def test_error_not_raised_on_valid_codes(self):
        """
        If all test codes are valid, the function should return the same
        format dict of the manifest -> test codes as is passed in, therefore
        test that this is true
        """
        tested_manifest, _ = utils.check_manifest_valid_test_codes(
            manifest=self.manifest, genepanels=self.genepanels
        )

        assert tested_manifest.items() == self.manifest.items(), (
            "Manifest changed when checking test codes with valid test codes"
        )

    def test_error_raised_when_manifest_contains_invalid_test_code(self):
        """
        RuntimeError should be raised if an invalid test code is provided
        in the manifest, check that the correct error is returned
        """
        # add in an invalid test code to a manifest sample
        manifest_copy = deepcopy(self.manifest)
        manifest_copy['424487111-53214R00111']['tests'].append([
            'invalidTestCode'])

        correct_error = (
            'One or more samples had an invalid test code requested:\n\t{'
            '\n\t424487111-53214R00111": [\n\t\t"invalid_test"\n\t]\t}'
        )

        with pytest.raises(RuntimeError):
            tested_manifest, _ = utils.check_manifest_valid_test_codes(
                manifest=manifest_copy, genepanels=self.genepanels
            )

    def test_error_not_raised_when_research_use_test_code_present(self):
        """
        Sometimes from Epic 'Research Use' can be present in the Test Codes
        column, we want to skip these as they're not a valid test code and
        not raise an error
        """
        # add in 'Research Use' as a test code to a manifest sample
        manifest_copy = deepcopy(self.manifest)
        manifest_copy['424487111-53214R00111']['tests'].append([
            'Research Use'])

        correct_test_codes = [['R208.1', 'R216.1']]

        tested_manifest, _ = utils.check_manifest_valid_test_codes(
            manifest=manifest_copy, genepanels=self.genepanels
        )
        sample_test_codes = tested_manifest['424487111-53214R00111']['tests']

        assert sample_test_codes == correct_test_codes, (
            'Test codes not correctly parsed when "Research Use" present'
        )
        


class TestSplitManifestTests():
    """
    TODO
    """


class TestAddPanelsAndIndicationsToManifest():
    """
    TODO
    """
