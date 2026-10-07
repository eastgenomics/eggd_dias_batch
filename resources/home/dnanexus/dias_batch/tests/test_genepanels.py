"""
Tests for functions in utils/genepanels.py
"""
from copy import deepcopy
import os
import subprocess
import sys

import pandas as pd
import pytest


sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))

from utils import genepanels


TEST_DATA_DIR = (
    os.path.join(os.path.dirname(__file__), 'test_data')
)


class TestParseGenePanels():
    """
    Tests for genepanels.parse_genepanels() that reads in the genepanels
    file, drops the HGNC ID column and keeps the unique rows left (i.e.
    one row per clinical indication / panel), and adds the test code as
    a separate column.
    """
    with open(f"{TEST_DATA_DIR}/genepanels.tsv") as file_handle:
        # parse genepanels file like is done in dias_batch.main()
        genepanels_data = file_handle.read().splitlines()
        genepanels_df = genepanels.parse_genepanels(genepanels_data)

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
    Tests for genepanels.split_genepanels_test_codes()

    Function takes the read in genepanels file and splits out the test code
    that prefixes the clinical indication (i.e. R337.1 -> R337.1_CADASIL_G)
    """
    # read in genepanels file in the same manner as genepanels.parse_genepanels()
    # up to the point of calling split_gene_panels_test_codes()
    with open(f"{TEST_DATA_DIR}/genepanels.tsv") as file_handle:
        # parse genepanels file like is done in dias_batch.main()
        genepanels_data = file_handle.read().splitlines()
        genepanels_df = pd.DataFrame(
            [x.split('\t') for x in genepanels_data],
            columns=['indication', 'panel_name', 'hgnc_id']
        )
        genepanels_df.drop(columns=['hgnc_id'], inplace=True)  # chuck away HGNC ID
        genepanels_df.drop_duplicates(keep='first', inplace=True)
        genepanels_df.reset_index(inplace=True)


    def test_genepanels_unchanged_by_splitting(self):
        """
        Test that no rows get added or removed
        """
        panel_df = genepanels.split_genepanels_test_codes(self.genepanels_df)

        current_indications = self.genepanels_df['indication'].tolist()
        split_indications = panel_df['indication'].tolist()

        assert current_indications == split_indications, (
            'genepanels indications changed when splitting test codes'
        )

    def test_splitting_r_code(self):
        """
        Test splitting of R code from a clinical indication works
        """
        panel_df = genepanels.split_genepanels_test_codes(self.genepanels_df)
        r337_code = panel_df[panel_df['indication'] == 'R337.1_CADASIL_G']

        assert r337_code['test_code'].tolist() == ['R337.1'], (
            "Incorrect R test code parsed from clinical indication"
        )

    def test_splitting_c_code(self):
        """
        Test splitting of C code from a clinical indication works
        """
        panel_df = genepanels.split_genepanels_test_codes(self.genepanels_df)
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
        genepanels_copy = deepcopy(self.genepanels_df)
        genepanels_copy = pd.concat([genepanels_copy,
            pd.DataFrame([{
                'test_code': 'R337.1',
                'indication': 'R337.1_CADASIL_G_COPY',
                'panel_name': 'R337.1_CADASIL_G_COPY'
            }])
        ])

        with pytest.raises(RuntimeError):
            genepanels.split_genepanels_test_codes(genepanels_copy)
