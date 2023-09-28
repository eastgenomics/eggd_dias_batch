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
    Tests for utils.filter_manifest_samples_by_files()

    Function filters out sample names that either don't meet a specified
    regex pattern (i.e. [\w\d]+-[\w\d]+- for Epic sample naming) or have
    no files available against a list of samples found to use as inputs  
    """
    with open(os.path.join(TEST_DATA_DIR, 'epic_manifest.txt')) as file_handle:
        epic_data = file_handle.read().splitlines()
        manifest, _ = utils.parse_manifest(epic_data)

    # minimal dxpy.find_data_objects() return object with list of files
    files = [
        {'describe': {
            'name': '123245111-23146R00111-other-name-parts_markdup.vcf.gz'}},
        {'describe': {
            'name': '224289111-33202R00111-other-name-parts_markdup.vcf.gz'}},
        {'describe': {
            'name': '324338111-43206R00111-other-name-parts_markdup.vcf.gz'}},
        {'describe': {
            'name': '424487111-53214R00111-other-name-parts_markdup.vcf.gz'}},
        {'describe': {
            'name': 'X225111-GM2308111-other-name-parts_markdup.vcf.gz'}}
    ]


    def test_files_not_matching_pattern_filtered_out(self, capsys):
        """
        Test where file name not matching the specified pattern is
        correctly filtered out
        """
        # add in a non-matching file
        file_list = deepcopy(self.files)
        file_list.append(
            {'describe': {'name': 'file1.txt'}}
        )

        utils.filter_manifest_samples_by_files(
            manifest=self.manifest,
            files=file_list,
            name='vcf',
            pattern=r'^[\w\d]+-[\w\d]+'  # matches 424487111-53214R00111-xxx
        )

        # since we don't explicitly return the filtered out files we
        # have to pick it out of the stdout as it just gets printed
        stdout = capsys.readouterr().out
        expected_files = 'Total files after filtering against pattern: 5'

        assert expected_files in stdout, (
            'Files not matching given pattern not correctly filtered out'
        )


    def test_samples_not_matching_pattern_filtered_out(self):
        """
        Test where sample name not matching the specified pattern is
        correctly filtered out
        """
        # bodge one of the sample names in the manifest
        manifest_copy = deepcopy(self.manifest)
        manifest_copy['invalid_sample_name'] = manifest_copy.pop(
            '424487111-53214R00111'
        )

        _, pattern_no_match, _ = utils.filter_manifest_samples_by_files(
            manifest=manifest_copy,
            files=self.files,
            name='vcf',
            pattern=r'^[\w\d]+-[\w\d]+'  # matches 424487111-53214R00111-xxx
        )

        assert pattern_no_match == ['invalid_sample_name'], (
            'Invalid sample name not correctly filtered out of manifest'
        )


    def test_sample_in_manifest_has_no_files(self):
        """
        Test where sample has no files that it gets removed from the manifest
        """
        manifest, _, sample_no_files  = utils.filter_manifest_samples_by_files(
            manifest=self.manifest,
            files=self.files[1:],  # exclude file for 123245111-23146R00111
            name='vcf',
            pattern=r'^[\w\d]+-[\w\d]+'  # matches 424487111-53214R00111-xxx
        )

        errors = []

        if not sample_no_files == ['123245111-23146R00111']:
            errors.append(
                'Sample with no file not correctly added to '
                'returned manifest_no_files list'
            )
        
        if '123245111-23146R00111' in manifest.keys():
            errors.append(
                'Sample with no file not correctly excluded from manifest'
            )
        
        assert not errors, errors


    def test_files_correctly_added_to_manifest(self):
        """
        Test that files correctly get added into manifest under specified key
        """
        manifest_w_files = {
            '123245111-23146R00111':  {
                'tests': [['R207.1']],
                'vcf': [{'describe': {
                    'name': '123245111-23146R00111-other-name-parts_markdup.vcf.gz'
                }}]
            },
            '224289111-33202R00111':  {
                'tests': [['R208.1']],
                'vcf': [{'describe': {
                    'name': '224289111-33202R00111-other-name-parts_markdup.vcf.gz'
                }}]
            },
            '324338111-43206R00111':  {
                'tests': [['R134.1']],
                'vcf': [{'describe': {
                    'name': '324338111-43206R00111-other-name-parts_markdup.vcf.gz'
                }}]
            },
            '424487111-53214R00111':  {
                'tests': [['R208.1', 'R216.1']],
                'vcf': [{'describe': {
                    'name': '424487111-53214R00111-other-name-parts_markdup.vcf.gz'
                }}]
            },
            'X225111-GM2308111':  {
                'tests': [['R149.1']],
                'vcf': [{'describe': {
                    'name': 'X225111-GM2308111-other-name-parts_markdup.vcf.gz'
                }}]
            }
        }


        manifest, _, _ = utils.filter_manifest_samples_by_files(
            manifest=self.manifest,
            files=self.files,
            name='vcf',
            pattern=r'^[\w\d]+-[\w\d]+'  # matches 424487111-53214R00111-xxx
        )

        assert manifest == manifest_w_files, (
            'files incorrectly added to manifest'
        )


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
        tested_manifest = utils.check_manifest_valid_test_codes(
            manifest=self.manifest, genepanels=self.genepanels
        )

        assert tested_manifest.items() == self.manifest.items(), (
            "Manifest changed when checking test codes with valid test codes"
        )

    def test_error_raised_when_sample_has_no_tests(self):
        """
        Test we raise an error if a sample has no test codes booked against it
        """
        # drop test codes for a manifest sample
        manifest_copy = deepcopy(self.manifest)
        manifest_copy['424487111-53214R00111']['tests'] = [[]]

        with pytest.raises(RuntimeError, match=r"No tests booked for sample"):
            utils.check_manifest_valid_test_codes(
                manifest=manifest_copy, genepanels=self.genepanels
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

        with pytest.raises(RuntimeError, match=r"invalidTestCode"):
            utils.check_manifest_valid_test_codes(
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

        tested_manifest = utils.check_manifest_valid_test_codes(
            manifest=manifest_copy, genepanels=self.genepanels
        )
        sample_test_codes = tested_manifest['424487111-53214R00111']['tests']

        assert sample_test_codes == correct_test_codes, (
            'Test codes not correctly parsed when "Research Use" present'
        )


class TestSplitManifestTests():
    """
    Tests for utils.split_manifest_tests()

    Function parses through the list of lists of test codes for each sample
    in the manifest and splits all test codes to be their own list, which
    will result in them generating their own reports
    """

    def test_panels_correctly_split_out(self):
        """
        Test that any panels are correctly split to their own test list
        """
        manifest = {
            "sample1" : {'tests': [['R1.1', 'R134.1']]},
            "sample2" : {'tests': [['R228.1']]},
            "sample3" : {'tests': [['R218.2'], ['R2.1']]},
        }

        split_manifest = utils.split_manifest_tests(manifest)

        correct_split = {
            "sample1" : {'tests': [['R1.1'], ['R134.1']]},
            "sample2" : {'tests': [['R228.1']]},
            "sample3" : {'tests': [['R218.2'], ['R2.1']]},
        }

        assert split_manifest == correct_split, (
            "Manifest test codes incorrectly split"
        )

    def test_gene_symbols_correctly_not_split(self):
        """
        Gene symbols requested together (i.e. in the same sub list of tests)
        should _not_ be split, but those not requested together (i.e. in
        different sub lists of tests) do _not_ get combined, test this works
        """
        manifest = {
            "sample1" : {'tests': [['_HGNC:235']]},
            "sample2" : {'tests': [['_HGNC:1623', '_HGNC:4401']]},
            "sample3" : {'tests': [['_HGNC:152'], ['_HGNC:18']]}
        }

        split_manifest = utils.split_manifest_tests(manifest)

        correct_split = manifest = {
            "sample1" : {'tests': [['_HGNC:235']]},
            "sample2" : {'tests': [['_HGNC:1623', '_HGNC:4401']]},
            "sample3" : {'tests': [['_HGNC:152'], ['_HGNC:18']]}
        }

        assert split_manifest == correct_split, (
            'Gene symbols incorrectly split'
        )

    def test_panels_and_gene_symbols_handled_together_correctly(self):
        """
        Combining the above to test mix of panels and gene symbols
        are correctly split
        """
        manifest = {
            "sample1" : {'tests': [['R1.1', 'R134.1', '_HGNC:235']]},
            "sample2" : {'tests': [['R228.1']]},
            "sample3" : {'tests': [['R218.2'], ['R2.1', '_HGNC:1623', '_HGNC:4401']]},
            "sample4" : {'tests': [['R1.1', '_HGNC:152'], ['R1.2', '_HGNC:18']]}
        }

        split_tests = utils.split_manifest_tests(manifest)

        correct_split = manifest = {
            "sample1" : {'tests': [['R1.1'], ['R134.1'], ['_HGNC:235']]},
            "sample2" : {'tests': [['R228.1']]},
            "sample3" : {'tests': [['R218.2'], ['R2.1'], ['_HGNC:1623', '_HGNC:4401']]},
            "sample4" : {'tests': [['R1.1'], ['_HGNC:152'], ['R1.2'], ['_HGNC:18']]}
        }

        assert split_tests == correct_split, (
            'Mix of panels and gene symbols incorrectly split'
        )


class TestAddPanelsAndIndicationsToManifest():
    """
    TODO
    """
