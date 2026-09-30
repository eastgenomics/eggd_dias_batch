"""
Tests for functions in utils/samples.py
"""
import os
import sys
from unittest.mock import patch

import pytest


sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))

from utils import samples


class TestCheckExcludeSamples():
    """
    Tests for samples.check_exclude_samples()

    Function checks the specified list of exclude samples against either
    list of BAM files (for CNV calling) or the manifest (CNV reports) to
    ensure all samples specified are valid for excluding
    """
    def test_no_error_raised_when_valid_samples_provided_to_exclude(self):
        """
        Test that when samples provided to exclude are in the list of
        sample files, no error is raised as expected
        """
        sample_list = [
            'sample1.bam',
            'sample2.bam',
            'sample3.bam'
        ]

        exclude = ['sample1', 'sample2']

        samples.check_exclude_samples(
            samples=sample_list,
            exclude=exclude,
            mode='calling'
        )


    def test_error_raised_when_no_bam_files(self):
        """
        Test when sample specified to exclude has no BAM files
        found, check will be when running for CNV calling
        """
        sample_list = [
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
            samples.check_exclude_samples(
                samples=sample_list,
                exclude=exclude,
                mode='calling'
            )


    def test_error_raised_when_sample_not_in_manifest(self):
        """
        Test error raised when sample specified to exclude not in
        the samples parsed from the manifest, check will be running
        when CNV calling
        """
        sample_list = [
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
            samples.check_exclude_samples(
                samples=sample_list,
                exclude=exclude,
                mode='reports'
            )

    @patch('utils.samples.dxpy.find_data_objects')
    def test_excluded_samples_not_in_manifest_but_have_bam_files(
            self,
            mock_find
        ):
        """
        When running CNV reports, samples specified to exclude may not
        be in the manifest (i.e when they have failed), for these we
        have an additional check for if they have a BAM file in the
        single output directory. If they do, they pass the check for
        being a valid sample to exclude and not a typo.
        """
        sample_list = [
            'sample1-a',
            'sample2-b',
            'sample-c'
        ]

        exclude = ['sample-d']

        # minimal dx find data return for sample BAM file being present
        mock_find.return_value = [
            {
                "id": "file-xxx",
                "describe": {
                    "name": "sample-d_some_suffixes.bam"
                }
            }
        ]

        # no error should be raised
        samples.check_exclude_samples(
            samples=sample_list,
            exclude=exclude,
            mode='reports',
            single_dir='project-xxx:/output/runX'
        )


    @patch('utils.samples.dxpy.find_data_objects')
    def test_excluded_samples_not_in_manifest_and_have_no_bam_files(
            self,
            mock_find
        ):
        """
        When running CNV reports, samples specified to exclude may not
        be in the manifest (i.e when they have failed), for these we
        have an additional check for if they have a BAM file in the
        single output directory.
        If they do not, they will not pass the check for being a valid
        sample to exclude and should still raise a RuntimeError
        """
        sample_list = [
            'sample1-a',
            'sample2-b',
            'sample-c'
        ]

        exclude = ['sample-d']

        # empty dx find data return for sample with no BAM file
        mock_find.return_value = []

        expected_error = (
            "samples provided to exclude from CNV reports not "
            r"valid: \['sample-d'\]"
        )

        with pytest.raises(RuntimeError, match=expected_error):
            samples.check_exclude_samples(
                samples=sample_list,
                exclude=exclude,
                mode='reports',
                single_dir='project-xxx:/output/runX'
            )


    @patch('utils.samples.dxpy.find_data_objects')
    def test_control_sample_pattern_not_checked_against_files(
            self,
            mock_find,
            capsys
        ):
        """
        Test when control sample pattern (from `-iexclude_controls`) is
        in the list of exclude patterns that it is ignored when checking.

        This is because controls may not be always on a run and it is a
        fixed pattern, therefore it is not specified by the user directly.

        Here we just want to test when it is the only pattern that it is
        removed from the list to check, and therefore it will pass the check
        """
        sample_list = [
            'sample1.bam',
            'sample2.bam',
            'sample3.bam'
        ]

        exclude = [r'^\w+-\w+Q\w+-']

        samples.check_exclude_samples(
            samples=sample_list,
            exclude=exclude,
            mode='calling'
        )

        stdout = capsys.readouterr().out

        assert 'All exclude sample names valid' in stdout, (
            'regex control pattern not correctly removed from checking'
        )
