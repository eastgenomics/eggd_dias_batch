"""
Tests for functions in utils/manifest.py
"""
from copy import deepcopy
import os
import re
import sys

import pytest


sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))

from utils import genepanels as genepanels_module
from utils import manifest


TEST_DATA_DIR = (
    os.path.join(os.path.dirname(__file__), 'test_data')
)


class TestParseManifest:
    """
    Tests for manifest.parse_manifest()

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
            manifest.parse_manifest(data)


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
        parsed_manifest, _ = manifest.parse_manifest(self.gemini_data)

        correct_tests = [['_HGNC:795', '_HGNC:16627', 'R228.1']]

        assert parsed_manifest['X223441']['tests'] == correct_tests, (
            "Multiple tests in Gemini manifest not correctly merged"
        )


    def test_gemini_invalid_codes_added_as_is(self):
        """
        Invalid test codes will be gathered up and raised as a single error
        in manifest.check_manifest_valid_test_codes, check that if an
        invalid code is provided that it is written out as found
        """
        data = deepcopy(self.gemini_data)
        data.append('anotherSample\tnotValidTest')

        parsed_manifest, _ = manifest.parse_manifest(data)

        assert parsed_manifest['anotherSample']['tests'] == [['notValidTest']], (
            'Invalid test code not kept correctly in manifest'
        )


    def test_epic_correct_number_lines_parsed(self):
        """
        Test when reading Epic manifest we get the correct number of
        lines since we drop first row (batch ID) and second (column names)
        """
        parsed_manifest, _  = manifest.parse_manifest(self.epic_data)

        assert len(parsed_manifest.keys()) == 5, (
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
                manifest.parse_manifest(data)


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
        parsed_manifest, _ = manifest.parse_manifest(data)

        errors = []

        if any([' ' in x for x in parsed_manifest.keys()]):
            errors.append('Spaces in sample ID')

        if any(['SP' in x for x in parsed_manifest.keys()]):
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
        parsed_manifest, _ = manifest.parse_manifest(self.epic_data)
        assert 'X225111-GM2308111' in parsed_manifest.keys(), (
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
            manifest.parse_manifest(data)


    def test_epic_invalid_sample_id_skipped_when_subset_specified(self):
        """
        Where both ReanalysisID and SampleID are not valid, this would
        normally raise a RuntimeError (as tested in test_epic_missing_
        sample_id_caught()). If the subset param is specified these
        should be skipped and only checked if any of the samples
        specified to subset do not exist in the resultant manifest
        """
        data = deepcopy(self.epic_data)

        # remove the specimen ID for the first sample to make it invalid
        # row 2 => first row of sample data w/ normal specimen - instrument ID
        data[2] = ';'.join([
            '' if idx == 2 else x for idx, x in enumerate(data[2].split(';'))
        ])

        manifest.parse_manifest(
            data, subset='224289111-33202R00111,324338111-43206R00111'
        )


    def test_epic_invalid_sample_id_skipped_and_subset_checked(self):
        """
        As testing above in test_epic_invalid_sample_id_skipped_when_
        subset_specified() - but now we want to test where the invalid
        sample in the manifest that is skipped is specified in the
        subset and we catch this and raise a RuntimeError
        """
        data = deepcopy(self.epic_data)

        # remove the specimen ID for the first sample to make it invalid
        # row 2 => first row of sample data w/ normal specimen - instrument ID
        data[2] = ';'.join([
            '' if idx == 2 else x for idx, x in enumerate(data[2].split(';'))
        ])

        expected_error = re.escape(
            "Sample names provided to -isubset not in manifest: "
            "['123245111-23146R00111']"
        )

        with pytest.raises(RuntimeError, match=expected_error):
            # 123245111-23146R00111 provided to subset is missing the
            # specimenID as removed above - make sure we catch this
            manifest.parse_manifest(
                data, subset='224289111-33202R00111,123245111-23146R00111'
            )


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
            manifest.parse_manifest(data)


    def test_split_tests_called(self):
        """
        Test when split_tests specified it gets called
        """
        parsed_manifest, _ = manifest.parse_manifest(
            contents=self.epic_data,
            split_tests=True
        )

        assert parsed_manifest['424487111-53214R00111']['tests'] == [
            ['R208.1'], ['R216.1']
        ], (
            'Splitting tests when parsing manifest not as expected'
        )


    def test_subset_works(self):
        """
        Test when subset is specified that it subsets the manifest
        """
        parsed_manifest, _ = manifest.parse_manifest(
            contents=self.epic_data,
            split_tests=True,
            subset='123245111-23146R00111,424487111-53214R00111'
        )

        assert sorted(parsed_manifest.keys()) == [
            '123245111-23146R00111', '424487111-53214R00111'
        ], ('Manifest not subsetted correctly')


    def test_error_raised_on_invalid_sample_provided_to_subset(self):
        """
        Test when a sample provided to subset is not in the manifest
        that an error is raised
        """
        with pytest.raises(
            RuntimeError,
            match=r"Sample names provided to -isubset not in manifest: \['sample1'\]"
        ):
            manifest.parse_manifest(
            contents=self.epic_data,
            split_tests=True,
            subset='123245111-23146R00111,sample1'
        )


class TestFilterManifestSamplesByFiles():
    """
    Tests for manifest.filter_manifest_samples_by_files()

    Function filters out sample names that either don't meet a specified
    regex pattern (i.e. [\w\d]+-[\w\d]+- for Epic sample naming) or have
    no files available against a list of samples found to use as inputs
    """
    with open(os.path.join(TEST_DATA_DIR, 'epic_manifest.txt')) as file_handle:
        epic_data = file_handle.read().splitlines()
        epic_manifest, _ = manifest.parse_manifest(epic_data)

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

        manifest.filter_manifest_samples_by_files(
            manifest=self.epic_manifest,
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
        manifest_copy = deepcopy(self.epic_manifest)
        manifest_copy['invalid_sample_name'] = manifest_copy.pop(
            '424487111-53214R00111'
        )

        _, pattern_no_match, _ = manifest.filter_manifest_samples_by_files(
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
        result_manifest, _, sample_no_files  = manifest.filter_manifest_samples_by_files(
            manifest=self.epic_manifest,
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

        if '123245111-23146R00111' in result_manifest.keys():
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


        result_manifest, _, _ = manifest.filter_manifest_samples_by_files(
            manifest=self.epic_manifest,
            files=self.files,
            name='vcf',
            pattern=r'^[\w\d]+-[\w\d]+'  # matches 424487111-53214R00111-xxx
        )

        assert result_manifest == manifest_w_files, (
            'files incorrectly added to manifest'
        )


class TestCheckManifestValidTestCodes():
    """
    Tests for manifest.check_manifest_valid_test_codes()

    Function parses through all test codes from the manifest, and checks
    they are valid against what we have in genepanels. If any are invalid
    for any sample, an error is raised.
    """
    with open(os.path.join(TEST_DATA_DIR, 'epic_manifest.txt')) as file_handle:
        epic_data = file_handle.read().splitlines()
        epic_manifest, _ = manifest.parse_manifest(epic_data)

    # read in genepanels file
    with open(f"{TEST_DATA_DIR}/genepanels.tsv") as file_handle:
        # parse genepanels file like is done in dias_batch.main()
        genepanels_data = file_handle.read().splitlines()
        genepanels_df = genepanels_module.parse_genepanels(genepanels_data)


    def test_error_not_raised_on_valid_codes(self):
        """
        If all test codes are valid, the function should return the same
        format dict of the manifest -> test codes as is passed in, therefore
        test that this is true
        """
        tested_manifest = manifest.check_manifest_valid_test_codes(
            manifest=self.epic_manifest, genepanels=self.genepanels_df
        )

        assert tested_manifest.items() == self.epic_manifest.items(), (
            "Manifest changed when checking test codes with valid test codes"
        )

    def test_error_raised_when_sample_has_no_tests(self):
        """
        Test we raise an error if a sample has no test codes booked against it
        """
        # drop test codes for a manifest sample
        manifest_copy = deepcopy(self.epic_manifest)
        manifest_copy['324338111-43206R00111']['tests'] = []
        manifest_copy['424487111-53214R00111']['tests'] = [[]]

        expected_error = re.escape(
            "One or more samples had an invalid test code requested:"
            "\t324338111-43206R00111: ['No tests booked for sample']\n"
            "\t424487111-53214R00111: ['No tests booked for sample']"
        )

        with pytest.raises(RuntimeError, match=expected_error):
            manifest.check_manifest_valid_test_codes(
                manifest=manifest_copy, genepanels=self.genepanels_df
            )


    def test_error_raised_when_manifest_contains_invalid_test_code(self):
        """
        RuntimeError should be raised if an invalid test code is provided
        in the manifest, check that the correct error is returned
        """
        # add in an invalid test code to a manifest sample
        manifest_copy = deepcopy(self.epic_manifest)
        manifest_copy['424487111-53214R00111']['tests'].append([
            'invalidTestCode'])

        with pytest.raises(RuntimeError, match=r"invalidTestCode"):
            manifest.check_manifest_valid_test_codes(
                manifest=manifest_copy, genepanels=self.genepanels_df
            )

    def test_error_not_raised_when_research_use_test_code_present(self):
        """
        Sometimes from Epic 'Research Use' can be present in the Test Codes
        column, we want to skip these as they're not a valid test code and
        not raise an error
        """
        # add in different forms of 'Research Use' as a test code to a
        # manifest sample
        manifest_copy = deepcopy(self.epic_manifest)
        manifest_copy['424487111-53214R00111']['tests'].append([
            'Research Use', 'ResearchUse', 'researchUse', 'research use'
        ])

        correct_test_codes = [['R208.1', 'R216.1']]

        tested_manifest = manifest.check_manifest_valid_test_codes(
            manifest=manifest_copy, genepanels=self.genepanels_df
        )
        sample_test_codes = tested_manifest['424487111-53214R00111']['tests']

        assert sample_test_codes == correct_test_codes, (
            'Test codes not correctly parsed when "Research Use" present'
        )


class TestSplitManifestTests():
    """
    Tests for manifest.split_manifest_tests()

    Function parses through the list of lists of test codes for each sample
    in the manifest and splits all test codes to be their own list, which
    will result in them generating their own reports
    """

    def test_panels_correctly_split_out(self):
        """
        Test that any panels are correctly split to their own test list
        """
        data = {
            "sample1": {'tests': [['R1.1', 'R134.1']]},
            "sample2": {'tests': [['R228.1']]},
            "sample3": {'tests': [['R218.2'], ['R2.1']]},
        }

        split_manifest = manifest.split_manifest_tests(data)

        correct_split = {
            "sample1": {'tests': [['R1.1'], ['R134.1']]},
            "sample2": {'tests': [['R228.1']]},
            "sample3": {'tests': [['R218.2'], ['R2.1']]},
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
        data = {
            "sample1": {'tests': [['_HGNC:235']]},
            "sample2": {'tests': [['_HGNC:1623', '_HGNC:4401']]},
            "sample3": {'tests': [['_HGNC:152'], ['_HGNC:18']]}
        }

        split_manifest = manifest.split_manifest_tests(data)

        correct_split = {
            "sample1": {'tests': [['_HGNC:235']]},
            "sample2": {'tests': [['_HGNC:1623', '_HGNC:4401']]},
            "sample3": {'tests': [['_HGNC:152'], ['_HGNC:18']]}
        }

        assert split_manifest == correct_split, (
            'Gene symbols incorrectly split'
        )

    def test_panels_and_gene_symbols_handled_together_correctly(self):
        """
        Combining the above to test mix of panels and gene symbols
        are correctly split
        """
        data = {
            "sample1": {'tests': [['R1.1', 'R134.1', '_HGNC:235']]},
            "sample2": {'tests': [['R228.1']]},
            "sample3": {'tests': [['R218.2'], ['R2.1', '_HGNC:1623', '_HGNC:4401']]},
            "sample4": {'tests': [['R1.1', '_HGNC:152'], ['R1.2', '_HGNC:18']]}
        }

        split_tests = manifest.split_manifest_tests(data)

        correct_split = {
            "sample1": {'tests': [['R1.1'], ['R134.1'], ['_HGNC:235']]},
            "sample2": {'tests': [['R228.1']]},
            "sample3": {'tests': [['R218.2'], ['R2.1'], ['_HGNC:1623', '_HGNC:4401']]},
            "sample4": {'tests': [['R1.1'], ['_HGNC:152'], ['R1.2'], ['_HGNC:18']]}
        }

        assert split_tests == correct_split, (
            'Mix of panels and gene symbols incorrectly split'
        )


class TestAddPanelsAndIndicationsToManifest():
    """
    Tests for manifest.add_panel_and_indications_to_manifest()

    Function goes over the test list for each sample in the manifest and
    adds equal length lists of clinical indications and panel strings
    as separate keys to be used later for inputs such as names in reports etc.
    """
    with open(os.path.join(TEST_DATA_DIR, 'epic_manifest.txt')) as file_handle:
        epic_data = file_handle.read().splitlines()
        epic_manifest, _ = manifest.parse_manifest(epic_data)

    with open(f"{TEST_DATA_DIR}/genepanels.tsv") as file_handle:
        genepanels_data = file_handle.read().splitlines()
        genepanels_df = genepanels_module.parse_genepanels(genepanels_data)


    def test_invalid_test_code_caught(self):
        """
        Test if an invalid test code is present in the manifest that it gets
        caught when trying to select it from genepanels. This shouldn't
        happen as we test for valid test codes in
        manifest.check_manifest_valid_test_codes() but lets add another check
        in because why not, never trust the things coming from humans
        """
        manifest_copy = deepcopy(self.epic_manifest)
        manifest_copy['424487111-53214R00111']['tests'] = [['R10000000001.1']]

        with pytest.raises(
            AssertionError,
            match='Filtering genepanels for R10000000001.1 returned empty df'
        ):
            manifest.add_panels_and_indications_to_manifest(
                manifest=manifest_copy,
                genepanels=self.genepanels_df
            )


    def test_correct_indications_panels(self):
        """
        Test that the correct panel and indication strings are added in for
        our test manifest.

        This includes testing of joyous panels such as R208.1 which is a
        'single' gene panel that is actually has multiple panel entries in
        genepanels under the same indication, for these we just join them
        all together with ';' and return a single string of all of these
        as the panel name (this is ugo but is only used in displaying in
        the report and nobody has complained so far so ¯\_(ツ)_/¯)
        """
        result_manifest = manifest.add_panels_and_indications_to_manifest(
            manifest=self.epic_manifest,
            genepanels=self.genepanels_df
        )

        correct_manifest = {
            "123245111-23146R00111": {
                "tests": [["R207.1"]],
                "panels": [
                    ["Inherited ovarian cancer (without breast cancer)_4.0"]
                ],
                "indications": [[
                    "R207.1_Inherited ovarian cancer (without breast cancer)_P"
                ]]
            },
            "224289111-33202R00111": {
                "tests": [["R208.1"]],
                "panels": [
                    [
                        "HGNC:1100;HGNC:1101;HGNC:16627;HGNC:26144;HGNC:795;"
                        "HGNC:9820;HGNC:9823_SG_panel_1.0.0"
                    ]
                ],
                "indications": [
                    ["R208.1_Inherited breast cancer and ovarian cancer_P"]
                ]
            },
            "324338111-43206R00111": {
                "tests": [["R134.1"]],
                "panels": [
                    ["Familial hypercholesterolaemia (GMS)_2.0"]
                ],
                "indications": [
                    ["R134.1_Familial hypercholesterolaemia_P"]
                ]
            },
            "424487111-53214R00111": {
                "tests": [["R208.1", "R216.1"]],
                "panels": [
                    [
                        "HGNC:1100;HGNC:1101;HGNC:16627;HGNC:26144;HGNC:795;"
                        "HGNC:9820;HGNC:9823_SG_panel_1.0.0",
                        "HGNC:11998;HGNC:17284_SG_panel_1.0.0"
                    ]
                ],
                "indications": [
                    [
                        "R208.1_Inherited breast cancer and ovarian cancer_P",
                        "R216.1_Li Fraumeni Syndrome_P"
                    ]
                ]
            },
            "X225111-GM2308111": {
                "tests": [ ["R149.1"] ],
                "panels": [[
                    "Severe early-onset obesity_4.0"
                ]],
                "indications": [[
                    "R149.1_Severe early-onset obesity_P"
                ]]
            }
        }

        assert result_manifest == correct_manifest, (
            'Clinical indications and panels incorrectly added to manifest'
        )


    def test_hgnc_ids_added(self):
        """
        HGNC IDs should be added to clinical indications and panels lists
        as is, test this happens
        """
        manifest_copy = deepcopy(self.epic_manifest)
        manifest_copy['424487111-53214R00111']['tests'] = [['_HGNC:12345']]

        result_manifest = manifest.add_panels_and_indications_to_manifest(
            manifest=manifest_copy,
            genepanels=self.genepanels_df
        )

        correct_added = {
            'tests': [['_HGNC:12345']],
            'indications': [['_HGNC:12345']],
            'panels': [['_HGNC:12345']]
        }

        assert result_manifest['424487111-53214R00111'] == correct_added, (
            'Clinical indication and / or panel wrongly added for HGNC ID'
        )


    def test_error_raised_invalid_test(self):
        """
        Test RuntimeError raised if invalid test code makes it through
        """
        manifest_copy = deepcopy(self.epic_manifest)
        manifest_copy['424487111-53214R00111']['tests'] = [['invalidTestCode']]

        with pytest.raises(
            RuntimeError,
            match=(
                'Error occurred selecting test from genepanels '
                'for test invalidTestCode'
            )
        ):
            manifest.add_panels_and_indications_to_manifest(
                manifest=manifest_copy,
                genepanels=self.genepanels_df
            )
