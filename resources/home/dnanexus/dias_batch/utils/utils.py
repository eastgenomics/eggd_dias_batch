"""
General utils for parsing config, genepanels and manifest files
"""
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
import json
from pprint import PrettyPrinter
import re
from time import strftime, localtime
from typing import Tuple

import dxpy
import pandas as pd

# for prettier viewing in the logs
pd.set_option('display.max_rows', 200)
pd.set_option('max_colwidth', 1500)
PPRINT = PrettyPrinter(indent=2, width=1000).pprint


def time_stamp() -> str:
    """
    Returns string of date & time formatted as YYMMDD_HHMM

    Returns
    -------
    str
        String of current date and time as YYMMDD_HHMM
    """
    return datetime.now().strftime("%y%m%d_%H%M")


def prettier_print(thing) -> None:
    """
    Pretty print for nicer viewing in the logs since pprint does not
    do an amazing job visualising big dicts and long strings

    Parameters
    ----------
    thing : anything json dumpable
        thing to print
    """
    print(json.dumps(thing, indent=4))


def check_report_index(name, reports) -> int:
    """
    Check for a given output name prefix if there are any previous reports
    and increase the suffix index to +1

    Parameters
    ----------
    name : str
        prefix of sample name + test code + [SNV|CNV|mosaic]
    reports : list
        list of previous reports found

    Returns
    -------
    int
        suffix to add to report name
    """
    suffix = 0
    previous_reports = [x for x in reports if x.startswith(name)]

    if previous_reports:
        # some previous reports, try get highest suffix
        suffixes = [
            re.search(r'[\d]{1,2}.xlsx$', x) for x in previous_reports
            if re.search(r'[\d]{1,2}.xlsx$', x)
        ]

        if suffixes:
            # found something useful, if not we're just going to use 1
            suffix = max([
                int(x.group().replace('.xlsx', '')) for x in suffixes if x
            ])

    return suffix + 1


def write_summary_report(output, job, app, manifest=None, **summary) -> None:
    """
    Write output summary file with jobs launched and any errors etc.

    Parameters
    ----------
    output : str
        name for output file
    job : dict
        details from dxpy.describe() call on job ID
    app : dict
        details from dxpy.describe() call on app ID
    manifest : dict
        mapping of samples in manifest -> requested test codes
    summary : kwargs
        all possible named summary metrics to write

    Outputs
    -------
    {output}.txt file of launched job summary
    """
    print(f"\n \nWriting summary report to {output}")

    time = strftime('%Y-%m-%d %H:%M:%S', localtime(job['created'] / 1000))
    inputs = job['runInput']
    inputs = "\n\t".join([f"{x[0]}: {x[1]}" for x in sorted(inputs.items())])

    with open(output, 'w') as file_handle:
        file_handle.write(
            f"Jobs launched from {app['name']} ({app['version']}) at {time} "
            f"by {job['launchedBy'].replace('user-', '')} in {job['id']}\n"
        )

        file_handle.write(
            f"\nAssay config file used {summary.get('assay_config')['name']} "
            f"({summary.get('assay_config')['dxid']})\n"
        )

        file_handle.write(f"\nJob inputs:\n\t{inputs}\n")

        if manifest:
            file_handle.write(
                f"\nManifest(s) parsed: {job['runInput']['manifest_files']}\n"
            )
            file_handle.write(
                f"\nTotal number of samples in manifest: {len(manifest.keys())}\n"
            )

        if summary.get('excluded'):
            file_handle.write(
                "\nSamples specified to exclude from CNV calling and CNV "
                f"reports ({len(summary.get('excluded'))}): "
                f"{', '.join(sorted(summary.get('excluded')))}"
            )

        launched_jobs = '\n\t'.join([
            f"{k} : {len(v)} jobs" if len(v) > 1
            else f"{k} : {len(v)} job"
            for k, v in summary.get('launched_jobs').items()
        ])

        file_handle.write(f"\nTotal jobs launched:\n\t{launched_jobs}\n")

        report_summaries = {
            "snv_report_errors": "SNV",
            "cnv_report_errors": "CNV",
            "mosaic_report_errors": "mosaic"
        }

        # write summary of errors from each report stage if present
        for key, word in report_summaries.items():
            if summary.get(key):
                errors = '\n\t'.join([
                    f"{k} : {v}" for k, v in summary.get(key).items()
                ])
                file_handle.write(
                    f"\nErrors in launching {word} reports:\n\t{errors}\n"
                )

        # mush the report summary dicts together to make a pretty table
        outputs = {}
        if summary.get('cnv_report_summary'):
            outputs = {**outputs, **summary.get('cnv_report_summary')}
        if summary.get('snv_report_summary'):
            outputs = {**outputs, **summary.get('snv_report_summary')}
        if summary.get('mosaic_report_summary'):
            outputs = {**outputs, **summary.get('mosaic_report_summary')}

        if outputs:
            fancy_table = pd.DataFrame(outputs)
            fancy_table.fillna(value='-', inplace=True)
            fancy_table = fancy_table.to_markdown(tablefmt="grid")
            file_handle.write(
                f"\nReports created per sample:\n\n{fancy_table}"
            )

    # dump written file into logs
    print('\n'.join(open(output, 'r').read().splitlines()))


def make_path(*path) -> str:
    """
    Generate path-like string (i.e. for searching in DNAnexus) or
    when setting output folder for running an app

    Parameters
    ----------
    path : iterable
        strings of path parts to join

    Returns
    -------
    str
        nicely formatted path with leading and trailing forward slash
    """
    path = '/'.join([
        re.sub(r"project-[\d\w]+:", "", x).lstrip('/').rstrip('/')
        for x in path if x
    ])

    return f"/{path}/"


def fill_config_reference_inputs(config) -> dict:
    """
    Fill config file input fields for all workflow stages against the
    reference files stored in top level of config

    Parameters
    ----------
    config : dict
        assay config file

    Returns
    -------
    dict
        config with input files parsed in

    Raises
    ------
    RuntimeError
        Raised when provided reference in assay config has no file-[\d\w]+ ID
    """
    print("\n \nFilling config file with reference files, before:")
    prettier_print(config)

    print("Reference files to add:")
    prettier_print(config['reference_files'])

    filled_config = deepcopy(config)

    # empty so we can fill with new inputs
    for mode in filled_config['modes']:
        filled_config['modes'][mode]['inputs'] = {}

    for mode, mode_config in config['modes'].items():
        if not mode_config.get('inputs'):
            print(
                f"WARNING: {mode} in the config does not appear to "
                f"have an 'inputs' key, skipping adding reference files"
            )
            continue
        for input, value in mode_config['inputs'].items():
            match = False
            for reference, file_id in config['reference_files'].items():
                if not value == f'INPUT-{reference}':
                    continue

                # this input is a match => add this ref file ID as
                # the input and move to next input
                match = True

                if isinstance(file_id, dict):
                    # being provided as $dnanexus_link format, use it
                    # as is and assume its formatted correctly
                    filled_config['modes'][mode]['inputs'][input] = file_id

                if isinstance(file_id, str):
                    # provided as string (i.e. project-xxx:file-xxx)
                    project = re.search(r'project-[\d\w]+', file_id)
                    file = re.search(r'file-[\d\w]+', file_id)

                    # format correctly as dx link
                    if project and file:
                        dx_link = {
                            "$dnanexus_link": {
                                "project": project.group(),
                                "id": file.group()
                            }
                        }
                    elif file and not project:
                        dx_link = {"$dnanexus_link": file.group()}
                    else:
                        # not found a file ID
                        raise RuntimeError(
                            f"Provided reference doesn't appear "
                            f"valid: {reference} : {file_id}"
                        )

                    filled_config['modes'][mode]['inputs'][input] = dx_link

                break

            if not match:
                # this input isn't a reference file => add back as is
                filled_config['modes'][mode]['inputs'][input] = value

    print("And now it's filled:")
    prettier_print(filled_config)

    return filled_config


def parse_genepanels(contents) -> pd.DataFrame:
    """
    Parse genepanels file into nicely formatted DataFrame

    This will drop the HGNC ID column and keep the unique rows left (i.e.
    one row per clinical indication / panel), and adds the test code as
    a separate column.

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
    genepanels = pd.DataFrame(
        [x.split('\t') for x in contents],
        columns=['indication', 'panel_name', 'hgnc_id']
    )
    genepanels.drop(columns=['hgnc_id'], inplace=True)  # chuck away HGNC ID
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


def parse_manifest(contents, split_tests=False, subset=None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Parse manifest data from file read in DNAnexus

    Handles both Gemini and Epic style manifest files

    Parameters
    ----------
    contents : list
        manifest file data
    split_tests : bool
        controls if to split multiple tests to be generated
        into separate reports
    subset : str
        comma separated string of sample names on which to ONLY run jobs

    Returns
    -------
    dict
        mapping of sampleID (str): 'tests': testCodes (list)
        e.g. {'sample1': {'tests': [['panel1']]}}
    dict
        mapping of sampleID (str): manifest_source (str; either 'Gemini'
        or 'Epic')

    Raises
    ------
    AssertionError
        Raised when Gemini manifest seems to have more than 2 columns
    AssertionError
        Raised when Epic manifest is missing one or more required columns
    RuntimeError
        Raised when a Epic sample name seems malformed (missing / wrongly
        formatted IDs)
    RuntimeError
        Raised when file doesn't appear to have either ';' or '\t' as delimiter
    RuntimeError
        Raised when sample names provided to subset are not in manifest
    """
    print(
        "\n \nParsing manifest file, file contents read from DNAnexus:\n\t",
        "\n\t".join(contents)
    )

    # turn manifest into a dict mapping sample ID to list of test codes,
    # duplicate samples in the same manifest for Epic samples will result
    # in >1 list of test codes , will be structured as:
    # {'sample1': {'tests': [['panel1', 'gene1'], ['panel2']]}}
    # for Gemini samples we will squash these down to a single list due
    # to how they are booked in and get split to multiple lines (it's going
    # away anyway so this is just for handling legacy samples)
    manifest_source = {}

    if all('\t' in x for x in contents if x):
        # this is an old Gemini manifest => should just have sampleID -> CI
        contents = [x.split('\t') for x in contents if x]

        source = 'Gemini'

        # sense check data does only have 2 columns
        assert all([len(x) == 2 for x in contents]), (
            f"Gemini manifest has more than 2 columns:\n\t{contents}"
        )

        # initialise a dict of sample names to add tests to
        sample_names = {x[0] for x in contents}
        data = {name: {'tests': [[]]} for name in sample_names}

        for sample, tests in contents:
            test_codes = tests.replace(' ', '').split(',')

            manifest_source[sample] = {'manifest_source': 'Gemini'}

            for test_code in test_codes:
                # add test codes to samples list, keeping just the code part
                # and not full string (i.e. R134.2 from
                # R134.1_Familialhypercholesterolaemia_P)
                match = re.match(r"[RC][\d]+\.[\d]+|_HGNC:[\d]+", test_code)
                if match:
                    code = match.group()
                else:
                    # this likely isn't valid, but will raise an error
                    # when we validate all test codes against genepanels
                    # in utils.check_manifest_valid_test_codes()
                    code = test_code

                data[sample]['tests'][0].append(code)

    elif all(';' in x for x in contents[1:] if x):
        # csv file => Epic style manifest
        # (not actually a csv file even though they call it .csv since it
        # has ; as a delimiter and everything is a lie)
        # first row is just batch ID and 2nd is column names
        contents = [x.split(';') for x in contents if x]
        manifest = pd.DataFrame(contents[2:], columns=contents[1])

        source = 'Epic'

        # sense check we have columns we need
        required = [
            'Instrument ID', 'Specimen ID', 'Re-analysis Instrument ID',
            'Re-analysis Specimen ID', 'Test Codes'
        ]

        assert not set(required) - set(manifest.columns.tolist()), (
            "Missing one or more required columns from Epic manifest"
        )

        # make sure we don't have any spaces from pesky humans
        # and their fat fingers
        columns = [
            'Instrument ID', 'Specimen ID', 'Re-analysis Instrument ID',
            'Re-analysis Specimen ID', 'Test Codes'
        ]

        # remove any spaces and SP- from specimen columns
        manifest[columns] = manifest[columns].applymap(
            lambda x: x.replace(' ', '') if x else x)
        manifest['Re-analysis Specimen ID'] = \
            manifest['Re-analysis Specimen ID'].str.replace(
                r'SP-|\.', '', regex=True)
        manifest['Specimen ID'] = \
            manifest['Specimen ID'].str.replace(r'SP-|\.', '', regex=True)

        # sample id may be split between 'Specimen ID' and 'Instrument ID' or
        # Re-analysis Specimen ID and Re-analysis Instrument ID columns, join
        # these as {InstrumentID-SpecimenID} to get a mapping of sample ID -> CI
        manifest['SampleID'] = manifest['Instrument ID'] + \
            '-' + manifest['Specimen ID']
        manifest['ReanalysisID'] = manifest['Re-analysis Instrument ID'] + \
            '-' + manifest['Re-analysis Specimen ID']

        manifest = manifest[['SampleID', 'ReanalysisID', 'Test Codes']]

        data = defaultdict(lambda: defaultdict(list))

        for idx, row in manifest.iterrows():
            # split test codes to list and sense check they're valid format
            # will be formatted as 'R211.1, , , ,' or 'HGNC:1234, , , ,' etc.
            test_codes = [
                x for x in row['Test Codes'].replace(' ', '').split(',') if x
            ]

            # preferentially use ReanalysisID if present
            if re.match(r"[\d\w]+-[\d\w]+", row.ReanalysisID):
                data[row.ReanalysisID]['tests'].append(test_codes)
                manifest_source[row.ReanalysisID] = {'manifest_source': 'Epic'}
            elif re.match(r"[\d\w]+-[\d\w]+", row.SampleID):
                data[row.SampleID]['tests'].append(test_codes)
                manifest_source[row.SampleID] = {'manifest_source': 'Epic'}
            else:
                # something funky with this sample naming
                raise RuntimeError(
                    f"Error in sample formatting of row {idx + 1} in manifest:"
                    f"\n\t{row}"
                )
    else:
        # throw an error here as something is up with the file
        raise RuntimeError("Manifest file provided does not seem valid")

    if subset:
        # subset specified, keep just these samples from manifest
        subset = subset.split(',')
        print(
            "Subsetting manifest and retaining only the "
            f"following samples: {subset}"
        )

        # check that provided sample names are in our manifest
        invalid = [x for x in subset if x not in data.keys()]

        if invalid:
            raise RuntimeError(
                f'Sample names provided to -isubset not in manifest: {invalid}'
            )

        data = {
            sample: tests for sample, tests in data.items()
            if sample in subset
        }

    if split_tests:
        data = split_manifest_tests(data)

    samples = ('\n\t').join([
        f"{x[0]} -> {x[1]['tests']}" for x in data.items()
    ])
    print(f"\n \n{source} manifest parsed:\n\t{samples}")

    return data, manifest_source


def filter_manifest_samples_by_files(
        manifest, files, name, pattern) -> Tuple[dict, list, list]:
    """
    Filter samples in manifest against those where required per sample
    files have been found with DXManage.find_files().

    Used where there may be required per sample files missing for a given
    sample (i.e. sample has failed or explicitly been excluded from running)

    Parameters
    ----------
    manifest : dict
        dict mapping sampleID -> testCodes from parse_manifest()
    files : list
        list of DXFile objects returned from DXMange.find_files()
    name : str
        name of file type to add as key to manifest dict
    pattern : str
        regex pattern for selecting parts of name to match on, i.e.
            (Gemini naming)
            manifest name : X12345
            vcf name      : X12345-GM12345_much_suffix.vcf.gz
            pattern       : r'^X[\d]+'

            (Epic naming)
            manifest_name : 124801362-23230R0131
            vcf name      : 124801362-23230R0131-23NGSCEN15-8128-M-96527.vcf.gz
            pattern       : r'^[\d\w]+-[\d\w]+'

    Returns
    -------
    dict
        subset of manifest mapping dict with samples removed that have
        no files and with DXFile objects added under '{name}' as a list
        for each sample where one or more files were found
    list
        list of sample IDs that didn't match the specified pattern
    list
        list of sample IDs that didn't match a file
    """
    # build mapping of prefix using given pattern to matching files
    # i.e. {'124801362-23230R0131': DXFileObject{'id': ...}}
    print(f"\n \nFiltering manifest samples against available {name} files")
    print(
        f"Total files before filtering against pattern "
        f"'{pattern}' : {len(files)}"
    )
    file_prefixes = defaultdict(list)

    for file in files:
        match = re.match(pattern, file['describe']['name'])
        if match:
            file_prefixes[match.group()].append(file)
    print(
        "Total files after filtering against pattern: "
        f"{len(file_prefixes.keys())}"
    )

    manifest_no_match = []
    manifest_no_files = []
    manifest_with_files = defaultdict(lambda: defaultdict(list))

    for sample in manifest.keys():
        match = re.match(pattern, sample)
        if not match:
            # sample ID doesn't match expected pattern
            print(
                f"Sample in manifest {sample} does not match expected "
                f"pattern: {pattern}, sample will be excluded from analysis"
            )
            manifest_no_match.append(sample)
        else:
            # we have prefix, try find matching files with same prefix
            sample_files = file_prefixes.get(match.group())
            if not sample_files:
                # found no files for this sample
                print(
                    f"No files found for {sample} using pattern {pattern}, "
                    f"prefix matched in samplename {match.group()}"
                )
                manifest_no_files.append(sample)
            else:
                # sample matches pattern and matches some file(s)
                manifest_with_files[sample] = manifest[sample]
                manifest_with_files[sample][name] = sample_files

    if manifest_no_match:
        print(
            f"{len(manifest_no_match)} samples in manifest didn't match "
            f"expected pattern of {pattern}: {manifest_no_match}"
        )

    if manifest_no_files:
        print(
            f"{len(manifest_no_files)} samples in manifest didn't "
            f"have any matching files: {manifest_no_files}"
        )

    return manifest_with_files, manifest_no_match, manifest_no_files


def check_manifest_valid_test_codes(manifest, genepanels) -> dict:
    """
    Parse through manifest dict of sampleID -> test codes to check
    all codes are valid and exclude those that are invalid against
    genepanels file

    Parameters
    ----------
    manifest : dict
        mapping of sampleID -> test codes
    genepanels : pd.DataFrame
        dataframe of genepanels file

    Returns
    -------
    dict
        dict of manifest with valid test codes

    Raises
    ------
    RuntimeError
        Raised if any invalid test codes requested for one or more samples
    """
    print("\n \nChecking test codes in manifest are valid...")
    invalid = defaultdict(list)
    valid = defaultdict(lambda: defaultdict(list))

    genepanels_test_codes = sorted(set(genepanels['test_code'].tolist()))

    print(f"Current valid test codes:\n\t{genepanels_test_codes}")

    for sample, test_codes in manifest.items():
        sample_invalid_test = []

        if test_codes['tests'] == [[]]:
            # sample has no booked tests => chuck it in the error bucket
            invalid[sample].append('No tests booked for sample')
            continue

        # test codes stored under 'tests' key and is a list of lists
        # dependent on what genes / panels have been requested
        for test_list in test_codes['tests']:
            valid_tests = []

            for test in test_list:
                if test in genepanels_test_codes or re.search(r'HGNC:[\d]+', test):
                    valid_tests.append(test)
                elif test == 'Research Use':
                    # more Epic weirdness, chuck these out but don't break
                    print(
                        f"WARNING: {sample} booked for 'Research Use' test, "
                        f"skipping this test code and continuing..."
                    )
                else:
                    sample_invalid_test.append(test)
            if valid_tests:
                # one or more requested test is in genepanels
                valid[sample]['tests'].append(sorted(set(valid_tests)))

        if sample_invalid_test:
            # sample had one or more invalid test code
            invalid[sample].extend(sample_invalid_test)

    if invalid:
        raise RuntimeError(
            f"One or more samples had an invalid test code requested: {invalid}"
        )
    else:
        print("All sample test codes valid!")

    return valid


def split_manifest_tests(data) -> dict:
    """
    Split test codes to individual items to generate separate reports
    instead of being combined

    Data structure before will be some form of: {
        "sample1" : {'tests': [['panel1', 'panel2', '_gene1']]},
        "sample2" : {'tests': [['panel3']]},
        "sample3" : {'tests': [['panel1'], ['panel2', 'gene2', 'gene3']]},
        "sample4" : {'tests': [['panel1' 'gene1'], ['panel2', 'gene2']]}
    }

    which will change to: {
        "sample1" : {'tests': [['panel1'], ['panel2'], ['_gene1']]},
        "sample2" : {'tests': [['panel3']]},
        "sample3" : {'tests': [['panel1'], ['panel2'], ['gene2', 'gene3']]},
        "sample4" : {'tests': [['panel1'], ['gene1'], ['panel2'], ['gene2']]}
    }

    n.b. genes in the same initial sub list will always be grouped together
    to not generate single gene reports (e.g. sample3 above), and where there
    are single genes in more than one sub list (i.e. from 2 different lines
    in the manifest) these will not be grouped into one (e.g. sample4 above)


    Parameters
    ----------
    data : dict
        mapping of SampleID : [[testCode1, testCode2]]

    Returns
    -------
    dict
        mapping of SampleID: 'tests': [[testCode1], [testCode2], ...]
    """
    split_data = defaultdict(lambda: defaultdict(list))

    for sample, test_codes in data.items():
        all_split_test_codes = []
        for test_list in test_codes['tests']:
            test_genes = []
            for sub_test in test_list:
                if re.match(r"[RC][\d]+\.[\d]+", sub_test):
                    # it's a panel => split it out
                    all_split_test_codes.append([sub_test])
                else:
                    # it's a gene, add these back to a list to group
                    test_genes.append(sub_test)
            if test_genes:
                # there were some single genes to test
                all_split_test_codes.append(sorted(set(test_genes)))

        split_data[sample]['tests'].extend(all_split_test_codes)

    return split_data


def add_panels_and_indications_to_manifest(manifest, genepanels) -> dict:
    """
    Add panel and clinical indication strings to the manifest dict.

    This adds in the panels and clinical indications for each test code
    to the manifest under the keys 'panels' and 'indications'. These will
    be structured the same as the tests list of lists, matching the order
    and length. This then allows combining these as strings when configuring
    inputs such as panel strings for generate_bed.

    Example manifest dict before:
    "X223201" : {
        'tests': [['R168.1', '_HGNC:20499'], ['R134.1']]
    }

    Example manifest dict after:
    "X223201" : {
        'tests': [['R168.1', '_HGNC:20499'], ['R134.1']],
        'panels: [
            ['Non-acute porphyrias_1.4', '_HGNC:20499'],
            ['Familial hypercholesterolaemia (GMS)_2.0']
        ],
        'indications': [
            ['R168.1_Non-acute porphyrias_P', '_HGNC:20499'],
            ['R134.1_Familial hypercholesterolaemia_P']
        ]
    }

    Parameters
    ----------
    manifest : dict
        sample -> tests mapping dict of manifest
    genepanels : pd.DataFrame
        dataframe of genepanels file

    Returns
    -------
    dict
        manifest dict with additional panel and indication strings

    Raises
    ------
    AssertionError
        Raised when given test code for sample could not be found in
        genepanels dataframe
    RuntimeError
        Raised when test doesn't appear to match valid R/C code or HGNC ID
    """
    print("\n \nFinding panels and clinical indications for tests")
    print("Manifest before")
    PPRINT(manifest)

    manifest_with_panels = {}

    for sample, values in manifest.items():
        sample_tests = {
            'tests': values['tests'],
            'panels' : [],
            'indications': []
        }
        for test_list in values['tests']:
            panels = []
            indications = []
            for test in test_list:
                if re.fullmatch(r'[RC][\d]+\.[\d]+', test):
                    # get genepanels row for current test prefix, should just
                    # be one since we dropped HGNC ID column and duplicates

                    # SPOILER: in older genepanels it isn't always 1:1 as we
                    # have 'single gene panels' (which aren't actually single
                    # genes as there's multiple but OH WELL). This is not a
                    # thing in Eris and there's only ~20, so for these we will
                    # just dump all the single gene 'panel' names into one
                    # and they can deal with that, example of this hot mess:
                    # test_code          indication                 panel_name
                    # R371.1  R371.1_Malignant hyperthermia_P  HGNC:10483_SG_panel_1.0.0
                    # R371.1  R371.1_Malignant hyperthermia_P   HGNC:1397_SG_panel_1.0.0
                    # R371.1  R371.1_Malignant hyperthermia_P  HGNC:28423_SG_panel_1.0.0
                    #
                    # which would result in:
                    # R371.1 -> HGNC:10483_SG_panel_1.0.0;HGNC:1397_SG_panel_1.0.0;HGNC:28423_SG_panel_1.0.0

                    genepanels_row = genepanels[genepanels['test_code'] == test]

                    assert not genepanels_row.empty, (
                        f"Filtering genepanels for {test} returned empty df"
                    )

                    if len(genepanels_row.index) > 1:
                        # munge the panel strings together to handle the above
                        print(
                            f'Test code {test} has >1 panel name assigned, '
                            f'these will be combined:\n\t{genepanels_row}'
                        )
                        panel_str = ';'.join(
                            genepanels_row['panel_name'].tolist()
                        )

                        # try clean up the panel string and drop
                        # duplicated _SG_panel_1.0.0
                        if '_SG_panel_1.0.0' in panel_str:
                            panel_str = (
                                f"{re.sub(r'_SG_panel_1.0.0', '', panel_str)}"
                                "_SG_panel_1.0.0"
                            )
                    else:
                        # this is nice and sane and 1:1
                        panel_str = genepanels_row.iloc[0].panel_name

                    panels.append(panel_str)
                    indications.append(genepanels_row.iloc[0].indication)

                elif re.fullmatch(r'_HGNC:[\d]+', test):
                    # add gene IDs as is to all lists
                    panels.append(test)
                    indications.append(test)
                else:
                    # we already validated earlier all the test codes so
                    # shouldn't get here
                    raise RuntimeError(
                        "Error occurred selecting test from genepanels for "
                        f"test {test}"
                    )
            sample_tests['panels'].append(panels)
            sample_tests['indications'].append(indications)

        manifest_with_panels[sample] = sample_tests

    print("Manifest after")
    PPRINT(manifest_with_panels)

    return manifest_with_panels


def check_exclude_samples(samples, exclude, mode) -> dict:
    """
    Exclude samples specified to either -iexclude_samples or
    -iexclude_samples_file from the manifest used for CNV calling
    and / or CNV reports

    Parameters
    ----------
    samples : list
        list of sample names, will either be list of bam files found
        (before CNV calling) or sample names from manifest (if called
        from CNV reports)
    exclude : list[str]
        list of sample names to exclude from generating reports (n.b.
        this is ONLY for CNV reports), will be formatted as
        InstrumentID-SpecimenID (i.e. [123245111-33202R00111, ...])
    mode : str
        calling | reports, used to add context to error message

    Raises
    -------
    RuntimeError
        Raised when one or more exclude_samples not present in sample list
    """
    exclude_not_present = [
        name for name in exclude
        if not any([sample.startswith(name) for sample in samples])
    ]

    if exclude_not_present:
        # provide some more info in logs for debugging
        print(
            f"Samples provided to exclude: {exclude}"
        )
        if mode == "calling":
            print(f"BAM files found to use for CNV calling: {samples}")
        else:
            print(f"Samples parsed from manifest: {samples}")

        print(
            "Samples specified to exclude that do not appear to be valid: "
            f"{exclude_not_present}"
        )

        raise RuntimeError(
            f"samples provided to exclude from CNV {mode} "
            f"not valid: {exclude_not_present}"
        )
