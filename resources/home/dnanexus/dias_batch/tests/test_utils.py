from copy import deepcopy
import os
import pytest
import re
import sys

import pandas as pd


sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))

from utils import utils


TEST_DATA_DIR = (
    os.path.join(os.path.dirname(__file__), 'test_data')
)


class TestMakePath():
    """
    Tests for utils.make_path(), expect it to take any number of strings
    with random '/' and format nicely as a path for DNAnexus queries,
    dropping project- prefix if present
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


class TestCheckReportIndex():
    """
    Tests for utils.check_report_index()
    """
    reports = [
        "X223420-GM2225190_SNV_1.xlsx",
        "X223420-GM2225190_SNV_2.xlsx",
        "X223420-GM2225190_CNV_1.xlsx"
    ]

    def test_max_suffix_snv(self):
        """
        Test that max suffix returned for SNV
        """
        suffix = utils.check_report_index(
            name="X223420-GM2225190_SNV",
            reports=self.reports
        )

        assert suffix == 3, "Wrong suffix returned for 2 previous reports"

    def test_max_suffix_cnv(self):
        """
        Test max suffix returned for CNV
        """
        suffix = utils.check_report_index(
            name="X223420-GM2225190_CNV",
            reports=self.reports
        )

        assert suffix == 2, "Wrong suffix returned for 1 previous report"

    def test_suffix_1_returned_no_previous_reports(self):
        """
        Test suffix '1' returned when sample has no previous reports
        """
        suffix = utils.check_report_index(
            name="sample_no_previous_reports",
            reports=self.reports
        )

        assert suffix == 1, "Wrong suffix returned for no previous reports"

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

        assert suffix == 1, "Wrong suffix returned for no report suffix"


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


    def test_gemini_invalid_test_code(self):
        """
        Test if an invalid test code or HGNC ID provided an error is raised
        """
        data = deepcopy(self.gemini_data)
        data.append('X12345\tnotValidTest')

        with pytest.raises(RuntimeError):
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
            # copy Epic data and drop out required column
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

        manifest, _ = utils.parse_manifest(data)

        errors = []

        if any([' ' in x for x in manifest.keys()]):
            errors.append('Spaces in sample ID')

        if any(['SP' in x for x in manifest.keys()]):
            errors.append('SP in Sample ID')

        assert not errors, errors


    def test_epic_invalid_test_code(self):
        """
        Test that an error is raised if an invalid test code is provided
        (n.b. this just checks against a regex pattern and not if its
        valid against genepanels, this is done in
        utils.check_manifest_valid_test_codes())
        """
        # add invalid test code to test codes of last row
        data = deepcopy(self.epic_data)
        data[-1] = f"{data[-1]}invalidTestCode"

        with pytest.raises(RuntimeError):
            utils.parse_manifest(data)


    def test_epic_reanalysis_ids_used(self):
        """
        Where 'Re-analysis Specimen ID' or 'Re-analysis Instrument ID'
        are specified these should be used over the Specimen Id and
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


class TestParseGenePanels():
    """
    Tests for utils.parse_genepanels()
    """
    with open(f"{TEST_DATA_DIR}/genepanels.tsv") as file_handle:
        # parse genepanels file like is done in dias_batch.main()
        genepanels_data = file_handle.read().splitlines()
        genepanels_df = utils.parse_genepanels(genepanels_data)
