"""
Tests for functions in utils/formatting.py
"""
from datetime import datetime
import os
import sys
from unittest.mock import patch


sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))

from utils import formatting


class TestTimeStamp():
    """
    Test for formatting.time_stamp()
    """

    @patch("utils.formatting.datetime")
    def test_correct_format(self, datetime_mock):
        """
        Test datetime is returned in format expected
        """
        # set datetime.now() to a fixed value
        datetime_mock.now.return_value = datetime(2013, 2, 1, 10, 9, 8)

        assert formatting.time_stamp() == '130201_1009', (
            "Wrong datetime format returned"
        )


class TestMakePath():
    """
    Tests for formatting.make_path()

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

        path = formatting.make_path(*path_parts)

        correct_path = (
            "/dir1/double_slash/prefix_slash/suffix_slash/no_slash/"
            "and_another/much_path/many_lines/"
        )

        assert path == correct_path, "Invalid path built"
