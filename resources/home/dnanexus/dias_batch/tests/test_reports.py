"""
Tests for functions in utils/reports.py
"""
from copy import deepcopy
import os
import sys
import unittest


sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))

from utils import reports


class TestCheckReportIndex():
    """
    Tests for reports.check_report_index()

    Function takes a list of files returned from DXManage.find_files()
    and a sample name stem, filters down to just xlsx reports for the
    given sample and then gets the highest index found from the int before
    the '.xlsx' suffix
    """
    reports_list = [
        "X223420-GM2225190_SNV_1.xlsx",
        "X223420-GM2225190_SNV_2.xlsx",
        "X223420-GM2225190_CNV_1.xlsx"
    ]

    def test_max_suffix_snv(self):
        """
        Test that max suffix returned for SNV is correct
        """
        suffix = reports.check_report_index(
            name="X223420-GM2225190_SNV",
            reports=self.reports_list
        )

        assert suffix == 3, (
            "Wrong suffix returned for sample with 2 previous reports"
        )

    def test_max_suffix_cnv(self):
        """
        Test max suffix returned for CNV is correct
        """
        suffix = reports.check_report_index(
            name="X223420-GM2225190_CNV",
            reports=self.reports_list
        )

        assert suffix == 2, (
            "Wrong suffix returned for sample with 1 previous report"
        )

    def test_suffix_1_returned_no_previous_reports(self):
        """
        Test suffix '1' returned when sample has no previous reports
        """
        suffix = reports.check_report_index(
            name="sample_no_previous_reports",
            reports=self.reports_list
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

        suffix = reports.check_report_index(
            name="X223420-GM2225190_SNV",
            reports=previous_reports
        )

        assert suffix == 1, (
            "Wrong suffix returned for sample with no previous report suffix"
        )


class TestWriteSummaryReport(unittest.TestCase):
    """
    Tests for reports.write_summary_report()

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

    provided_manifest_samples = [
        'X111111', 'X111112', 'X111113', 'X111114', 'X111115', 'X111116'
    ]

    excluded_samples = ['X111115', 'X111116']

    # example list of files excluded from CNV calling
    cnv_call_excluded_files = [
        'X111115.bam', 'X111115.bam.bai', 'X111116.bam', 'X111116.bam.bai'
    ]

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

    reports.write_summary_report(
        output='dias_batch_summary_test_report.txt',
        job=job_details,
        app=app_details,
        assay_config=assay_config,
        manifest=manifest,
        provided_manifest_samples=provided_manifest_samples,
        launched_jobs=launched_jobs,
        excluded=excluded_samples,
        cnv_call_excluded=cnv_call_excluded_files,
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
        end = self.summary_contents.index(
            'Total number of samples in provided manifest(s): 6')

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
            if x.startswith('Total number of samples in provided manifest(s):')
        ]

        assert int(samples[0][-1]) == 6, (
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


    def test_excluded_bam_files_from_cnv_calling(self):
        """
        Test that BAM files excluded from CNV calling are correctly written
        """
        excluded = [
            x for x in self.summary_contents
            if x.startswith('Files matched and excluded from CNV calling')
        ]

        correct_excluded = (
            'Files matched and excluded from CNV calling (4): '
            'X111115.bam, X111115.bam.bai, X111116.bam, X111116.bam.bai'
        )

        assert excluded[0] == correct_excluded, (
            'Excluded BAM files incorrectly written'
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


    def test_manifest_sample_lines_correctly_written(self):
        """
        Test that the lines we write to the summary for the samples
        originally in the provided manifest, the samples we ran jobs for
        and the samples removed (i.e. where they had Research Use test
        codes) are correctly written
        """
        expected_text = (
            "\nTotal number of samples in provided manifest(s): 6"
            "\nTotal number of samples processed from manifest(s): 4"
            "\nSamples from manifest(s) not processed (2): X111115, X111116"
        )

        assert expected_text in '\n'.join(self.summary_contents), (
            'Manifest details incorrect in summary text'
        )
