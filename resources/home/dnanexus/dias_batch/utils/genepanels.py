"""
Functions for parsing the genepanels file
"""
import re

import pandas as pd


# for prettier viewing in the logs
pd.set_option('display.max_rows', 200)
pd.set_option('max_colwidth', 1500)


def parse_genepanels(contents) -> pd.DataFrame:
    """
    Parse genepanels file into nicely formatted DataFrame

    This will keep the unique rows from the first 2 columns (i.e. one
    row per clinical indication / panel), and adds the test code as a
    separate column.

    Example resultant dataframe:

    +-----------+-----------------------+---------------------------+
    | test_code |      indication       |        panel_name         |
    +-----------+-----------------------+---------------------------+
    | C1.1      | C1.1_Inherited Stroke |  CUH_Inherited Stroke_1.0 |
    | C2.1      | C2.1_INSR             |  CUH_INSR_1.0             |
    +-----------+-----------------------+---------------------------+

    Parameters
    ----------
    contents : list
        contents of genepanels file read from DXManage.read_dxfile()

    Returns
    -------
    pd.DataFrame
        DataFrame of genepanels file
    """
    # genepanels file may have 3 or 4 columns as it can also contain HGNC
    # ID and PanelApp panel ID, just use the first 2 columns
    genepanels = pd.DataFrame(
        [x.split('\t')[:2] for x in contents],
        columns=['indication', 'panel_name']
    )
    genepanels.drop_duplicates(keep='first', inplace=True)
    genepanels.reset_index(inplace=True)
    genepanels = split_genepanels_test_codes(genepanels)

    return genepanels


def split_genepanels_test_codes(genepanels) -> pd.DataFrame:
    """
    Split out R/C codes from full CI name for easier matching
    against manifest

    +-----------------------+--------------------------+
    |      indication      |        panel_name         |
    +-----------------------+--------------------------+
    | C1.1_Inherited Stroke | CUH_Inherited Stroke_1.0 |
    | C2.1_INSR             | CUH_INSR_1.0             |
    +-----------------------+--------------------------+

                                    |
                                    ▼

    +-----------+-----------------------+---------------------------+
    | test_code |      indication      |        panel_name          |
    +-----------+-----------------------+---------------------------+
    | C1.1      | C1.1_Inherited Stroke |  CUH_Inherited Stroke_1.0 |
    | C2.1      | C2.1_INSR             |  CUH_INSR_1.0             |
    +-----------+-----------------------+---------------------------+


    Parameters
    ----------
    genepanels : pd.DataFrame
        dataframe of genepanels with 3 columns

    Returns
    -------
    pd.DataFrame
        genepanels with test code split to separate column

    Raises
    ------
    RuntimeError
        Raised when test code links to more than one clinical indication
    """
    genepanels['test_code'] = genepanels['indication'].apply(
        lambda x: x.split('_')[0] if re.match(r'[RC][\d]+\.[\d]+', x) else x
    )
    genepanels = genepanels[['test_code', 'indication', 'panel_name']]

    # sense check test code only points to one unique indication
    for code in set(genepanels['test_code'].tolist()):
        code_rows = genepanels[genepanels['test_code'] == code]
        if len(set(code_rows['indication'].tolist())) > 1:
            raise RuntimeError(
                f"Test code {code} linked to more than one indication in "
                f"genepanels!\n\t{code_rows['indication'].tolist()}"
            )

    print(f"Genepanels file: \n{genepanels}")

    return genepanels
