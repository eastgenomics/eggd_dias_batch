import os
import sys


sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))

from utils import utils


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

