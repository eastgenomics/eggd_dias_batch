"""
Tests for general functions in utils.py
"""
from copy import deepcopy
from datetime import datetime
import json
import os
import re
import subprocess
import sys
from unittest.mock import patch

import pandas as pd
import pytest


sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))

from utils import utils


TEST_DATA_DIR = (
    os.path.join(os.path.dirname(__file__), 'test_data')
)


class TestTimeStamp():
    """
    Test for utils.time_stamp()
    """

    @patch("utils.utils.datetime")
    def test_correct_format(self, datetime_mock):
        """
        Test datetime is returned in format expected
        """
        # set datetime.now() to a fixed value
        datetime_mock.now.return_value = datetime(2013, 2, 1, 10, 9, 8)

        assert utils.time_stamp() == '130201_1009', (
            "Wrong datetime format returned"
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


class TestWriteSummaryReport():
    """
    Tests for utils.write_summary_report()

    Function takes in a variable number of objects to write to the summary
    report, dependent on the mode(s) run and issue(s) identified
    """
    # minimal dx describe objects of app and job that would be passed
    # into the function from where it is called in dias_batch.main
    job_details = {
        'id': 'job-GZFXvYj4VjyggGq9xXKb6qp8',
        'name': 'eggd_dias_batch',
        'executable': 'app-GZ4v1Q849BP045XxfBF4VzJk',
        'executableName': 'eggd_dias_batch',
        'created': 1696017999226,
        'launchedBy': 'testUser',
        'runInput': {
            'assay': 'CEN',
            'single_output_dir': 'project-GZ025k04VjykZx3bKJ7YP837:/output/CEN-230719_1604/',
            'snv_reports': True,
            'cnv_reports': True,
            'cnv_call_job_id': 'job-GXvQjz04YXKx5ZPjk36B17j2',
            'artemis': True,
            'exclude_samples': 'X223256,X223376,X223332,X223384',
            'assay_config_file': {
                '$dnanexus_link': 'file-GZFV3184VjyV3JZgQx0GBkBz'
            },
            'manifest_files': 'manifest.txt',
            'qc_file': {
                '$dnanexus_link': 'file-GYPg1fj4YXKbxV560Zfy3XFv'
            }
        },
        'app': 'app-GZ4v1Q849BP045XxfBF4VzJk'
    }

    app_details = {
        'id': 'app-GZ4v1Q849BP045XxfBF4VzJk',
        'name': 'eggd_dias_batch',
        'version': '3.0.0'
    }

    # other objects gathered up in dias_batch.main to pass
    # to write summary report
    assay_config = {
        'name': 'test_assay_config.json',
        'dxid': 'file-GZFV3184VjyV3JZgQx0GBkBz'
    }

    launched_jobs = {
        'cnv_call': ['job1'],
        'snv_reports': ['job1', 'job2', 'job3'],
        'cnv_reports': ['job1', 'job2', 'job3']
    }

    manifest = {
        'X111111': {'tests': [['R134.1']]},
        'X111112': {'tests': [['R134.1']]},
        'X111113': {'tests': [['R134.1']]},
        'X111114': {'tests': [['R134.1']]}
    }

    excluded_samples = ['X111115', 'X111116']

    # example per mode errors returned from DXExecute.reports_workflow
    cnv_reports_errors = {
        "Samples in manifest with no VCF found (2)": ["X111117", "X111118"]
    }
    snv_reports_errors = {
        "Samples in manifest with no mosdepth files found (1)": ["X111119"]
    }

    # example per mode summaries with per sample report names written
    cnv_report_summary = {
        'CNV': {
            'X111111': ['X111111_R134.1_CNV_1'],
            'X111112': ['X111112_R134.1_CNV_1']
        }
    }
    snv_report_summary = {
        'SNV': {
            'X111111': ['X111111_R134.1_SNV_1'],
            'X111112': ['X111112_R134.1_SNV_1']
        }
    }
    mosaic_report_summary = {
        'mosaic': {
            'X111111': ['X111111_R134.1_mosaic_1']
        }
    }

    utils.write_summary_report(
        output='dias_batch_summary_test_report.txt',
        job=job_details,
        app=app_details,
        assay_config=assay_config,
        launched_jobs=launched_jobs,
        manifest=manifest,
        excluded=excluded_samples,
        snv_report_errors=snv_reports_errors,
        cnv_report_errors=cnv_reports_errors,
        cnv_report_summary=cnv_report_summary,
        snv_report_summary=snv_report_summary,
        mosaic_report_summary=mosaic_report_summary
    )

    # read back in written summary then delete
    with open('dias_batch_summary_test_report.txt') as file_handle:
        summary_contents = file_handle.read().splitlines()

    os.remove('dias_batch_summary_test_report.txt')


    def test_inputs_written_correctly(self):
        """
        Test job inputs written to file as taken from the job details
        """
        # job inputs written between lines 'Job inputs:' and
        # 'Total number of samples in manifest: 4'
        start = self.summary_contents.index('Job inputs:')
        end = self.summary_contents.index('Total number of samples in manifest: 4')

        written_inputs = self.summary_contents[start + 1: end]
        written_inputs = sorted([
            x.replace('\t', '') for x in written_inputs if x
        ])

        original_inputs = deepcopy(self.job_details['runInput'])
        original_inputs['Manifest(s) parsed'] = 'manifest.txt'

        original_inputs = sorted([
            f"{k}: {v}" for k, v in original_inputs.items()
        ])

        assert written_inputs == original_inputs, 'Inputs incorrectly written'


    def test_total_no_samples_written(self):
        """
        Test total no. samples from manifest written correctly
        """
        samples = [
            x for x in self.summary_contents
            if x.startswith('Total number of samples in manifest')
        ]

        assert int(samples[0][-1]) == 4, (
            'Total no. samples wrongly parsed from manifest'
        )


    def test_excluded_samples_correct(self):
        """
        Test that excluded samples provided is correctly written
        """
        excluded = [
            x for x in self.summary_contents
            if x.startswith('Samples specified to exclude')
        ]

        correct_excluded = (
            'Samples specified to exclude from CNV calling '
            'and CNV reports (2): X111115, X111116'
        )

        assert excluded[0] == correct_excluded, (
            'Excluded samples incorrectly written'
        )


    def test_error_summary(self):
        """
        Test that if errors were generated during launching of each modes
        jobs that these are written into the file
        """
        # get errors for SNV and CNV written to report
        snv_errors_idx = self.summary_contents.index(
            'Errors in launching SNV reports:'
        )
        cnv_errors_idx = self.summary_contents.index(
            'Errors in launching CNV reports:'
        )

        written_errors = self.summary_contents[
            snv_errors_idx:snv_errors_idx + 2
        ] + self.summary_contents[
            cnv_errors_idx:cnv_errors_idx + 2
        ]

        written_errors = [x.replace('\t', '') for x in written_errors if x]

        correct_errors = [
            "Errors in launching SNV reports:",
            "Samples in manifest with no mosdepth files found (1) : ['X111119']",
            "Errors in launching CNV reports:",
            "Samples in manifest with no VCF found (2) : ['X111117', 'X111118']"
        ]

        assert written_errors == correct_errors, (
            'Error summaries incorrectly written'
        )


    def test_report_summary(self):
        """
        Test when report summaries are passed that these correctly get
        formatted into a markdown table
        """

        # markdown table as it should be written to report (without spacing)
        # as this makes it huge
        correct_table = (
            '+---------+--------------------------+--------------------------+'
            '-----------------------------+||CNV|SNV|mosaic|+=========+='
            '=========================+==========================+============'
            '=================+|X111111|[X111111_R134.1_CNV_1]|[X111111_'
            'R134.1_SNV_1]|[X111111_R134.1_mosaic_1]|+---------+--------'
            '------------------+--------------------------+-------------------'
            '----------+|X111112|[X111112_R134.1_CNV_1]|[X111112_R134.1_'
            'SNV_1]|-|+---------+--------------------------+--------------'
            '------------+-----------------------------+'
        )

        # table should be last part of the report, drop spaces and quotes to
        # make comparing easier
        table_idx = self.summary_contents.index('Reports created per sample:')
        written_table = ''.join([
            x.replace(' ', '').replace("'", "")
            for x in self.summary_contents[table_idx+1:] if x
        ])

        assert written_table == correct_table, (
            "Summary table incorrectly written to report"
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

    def test_app_no_inputs(self, capsys):
        """
        Test when an app/workflow in the config has no inputs dict defined
        that we print a warning and continue
        """
        config_copy = deepcopy(self.config)
        config_copy['modes']['app1'] = {}

        utils.fill_config_reference_inputs(config_copy)
        stdout = capsys.readouterr().out

        correct_print = (
            "WARNING: app1 in the config does not appear to "
            "have an 'inputs' key, skipping adding reference files"
        )

        assert correct_print in stdout, (
            'App missing inputs did not print expected warning'
        )


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


    def test_gemini_invalid_codes_added_as_is(self):
        """
        Invalid test codes will be gathered up and raised as a single error
        in utils.check_manifest_valid_test_codes, check that if an invalid
        code is provided that it is written out as found
        """
        manifest = deepcopy(self.gemini_data)
        manifest.append('anotherSample\tnotValidTest')

        manifest, _ = utils.parse_manifest(manifest)

        assert manifest['anotherSample']['tests'] == [['notValidTest']], (
            'Invalid test code not kept correctly in manifest'
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


    def test_split_tests_called(self):
        """
        Test when split_tests specified it gets called
        """
        manifest, _ = utils.parse_manifest(
            contents=self.epic_data,
            split_tests=True
        )

        assert manifest['424487111-53214R00111']['tests'] == [
            ['R208.1'], ['R216.1']
        ], (
            'Splitting tests when parsing manifest not as expected'
        )


    def test_subset_works(self):
        """
        Test when subset is specified that it subsets the manifest
        """
        manifest, _ = utils.parse_manifest(
            contents=self.epic_data,
            split_tests=True,
            subset='123245111-23146R00111,424487111-53214R00111'
        )

        assert sorted(manifest.keys()) == [
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
            utils.parse_manifest(
            contents=self.epic_data,
            split_tests=True,
            subset='123245111-23146R00111,sample1'
        )


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

    # read in genepanels file
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
            "sample1": {'tests': [['R1.1', 'R134.1']]},
            "sample2": {'tests': [['R228.1']]},
            "sample3": {'tests': [['R218.2'], ['R2.1']]},
        }

        split_manifest = utils.split_manifest_tests(manifest)

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
        manifest = {
            "sample1": {'tests': [['_HGNC:235']]},
            "sample2": {'tests': [['_HGNC:1623', '_HGNC:4401']]},
            "sample3": {'tests': [['_HGNC:152'], ['_HGNC:18']]}
        }

        split_manifest = utils.split_manifest_tests(manifest)

        correct_split = manifest = {
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
        manifest = {
            "sample1": {'tests': [['R1.1', 'R134.1', '_HGNC:235']]},
            "sample2": {'tests': [['R228.1']]},
            "sample3": {'tests': [['R218.2'], ['R2.1', '_HGNC:1623', '_HGNC:4401']]},
            "sample4": {'tests': [['R1.1', '_HGNC:152'], ['R1.2', '_HGNC:18']]}
        }

        split_tests = utils.split_manifest_tests(manifest)

        correct_split = manifest = {
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
    Tests for utils.add_panel_and_indications_to_manifest()

    Function goes over the test list for each sample in the manifest and
    adds equal length lists of clinical indications and panel strings
    as separate keys to be used later for inputs such as names in reports etc.
    """
    with open(os.path.join(TEST_DATA_DIR, 'epic_manifest.txt')) as file_handle:
        epic_data = file_handle.read().splitlines()
        manifest, _ = utils.parse_manifest(epic_data)

    with open(f"{TEST_DATA_DIR}/genepanels.tsv") as file_handle:
        genepanels_data = file_handle.read().splitlines()
        genepanels = utils.parse_genepanels(genepanels_data)


    def test_invalid_test_code_caught(self):
        """
        Test if an invalid test code is present in the manifest that it gets
        caught when trying to select it from genepanels. This shouldn't
        happen as we test for valid test codes in
        utils.check_manifest_valid_test_codes() but lets add another check
        in because why not, never trust the things coming from humans
        """
        manifest_copy = deepcopy(self.manifest)
        manifest_copy['424487111-53214R00111']['tests'] = [['R10000000001.1']]

        with pytest.raises(
            AssertionError,
            match='Filtering genepanels for R10000000001.1 returned empty df'
        ):
            utils.add_panels_and_indications_to_manifest(
                manifest=manifest_copy,
                genepanels=self.genepanels
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
        manifest = utils.add_panels_and_indications_to_manifest(
            manifest=self.manifest,
            genepanels=self.genepanels
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

        assert manifest == correct_manifest, (
            'Clinical indications and panels incorrectly added to manifest'
        )


    def test_hgnc_ids_added(self):
        """
        HGNC IDs should be added to clinical indications and panels lists
        as is, test this happens
        """
        manifest = deepcopy(self.manifest)
        manifest['424487111-53214R00111']['tests'] = [['_HGNC:12345']]

        manifest = utils.add_panels_and_indications_to_manifest(
            manifest=manifest,
            genepanels=self.genepanels
        )

        correct_added = {
            'tests': [['_HGNC:12345']],
            'indications': [['_HGNC:12345']],
            'panels': [['_HGNC:12345']]
        }

        assert manifest['424487111-53214R00111'] == correct_added, (
            'Clinical indication and / or panel wrongly added for HGNC ID'
        )


    def test_error_raised_invalid_test(self):
        """
        Test RuntimeError raised if invalid test code makes it through
        """
        manifest = deepcopy(self.manifest)
        manifest['424487111-53214R00111']['tests'] = [['invalidTestCode']]

        with pytest.raises(
            RuntimeError,
            match=(
                'Error occurred selecting test from genepanels '
                'for test invalidTestCode'
            )
        ):
            utils.add_panels_and_indications_to_manifest(
                manifest=manifest,
                genepanels=self.genepanels
            )


class TestCheckExcludeSamples():
    """
    Tests for utils.check_exclude_samples()

    Function checks the specified list of exclude samples against either
    list of BAM files (for CNV calling) or the manifest (CNV reports) to
    ensure all samples specified are valid for excluding
    """
    def test_error_raised_when_no_bam_files(self):
        """
        Test when sample specified to exclude has no BAM files
        found, check will be when running for CNV calling
        """
        samples = [
            'sample1.bam',
            'sample2.bam',
            'sample3.bam'
        ]

        exclude = ['sample4']

        expected_error = (
            "samples provided to exclude from CNV calling not "
            r"valid: \['sample4'\]"
        )

        with pytest.raises(RuntimeError, match=expected_error):
            utils.check_exclude_samples(
                samples=samples,
                exclude=exclude,
                mode='calling'
            )

    def test_error_raised_when_sample_not_in_manifest(self):
        """
        Test error raised when sample specified to exclude not in
        the samples parsed from the manifest, check will be running
        when CNV calling
        """
        samples = [
            'sample1-a',
            'sample2-b',
            'sample-c'
        ]

        exclude = ['sample-d']

        expected_error = (
            "samples provided to exclude from CNV reports not "
            r"valid: \['sample-d'\]"
        )

        with pytest.raises(RuntimeError, match=expected_error):
            utils.check_exclude_samples(
                samples=samples,
                exclude=exclude,
                mode='reports'
            )
