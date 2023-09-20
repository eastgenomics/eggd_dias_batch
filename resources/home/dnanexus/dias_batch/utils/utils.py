"""
General utils for parsing config, genepanels and manifest files
"""
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
import json
from pprint import PrettyPrinter
import re
from typing import Tuple

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
        ]
        if suffixes:
            # found something useful, if not we're just going to use 1
            suffix = max([
                int(x.group().replace('.xlsx', '')) for x in suffixes if x
            ])

    print(f"Previous xlsx reports found for {name}: {bool(previous_reports)}")
    print(f"Using suffix: {suffix + 1}")

    return suffix + 1


def write_summary_report(output, manifest, **summary) -> None:
    """
    Write output summary file with jobs launched and any errors etc.

    Parameters
    ----------
    output : str
        name for output file
    manifest : dict
        mapping of samples in manifest -> requested test codes
    summary : kwargs
        all possible named summary metrics to write

    Outputs
    -------
    {output}.txt file of launched job summary
    """
    print(f"Writing summary report to {output}")
    with open(output, 'w') as file_handle:
        file_handle.write(
            f"Assay config file used {summary.get('assay_config')['name']} "
            f"({summary.get('assay_config')['dxid']})\n"
        )
        file_handle.write(
            f"\nTotal number of samples in manifest: {len(manifest.keys())}\n"
        )
        launched_jobs = '\n\t'.join([
            f"{k} : {len(v)} jobs" for k, v
            in summary.get('launched_jobs').items()
        ])
        file_handle.write(f"\nTotal jobs launched:\n\t{launched_jobs}\n")

        if summary.get('invalid_tests'):
            invalid_tests = '\n\t'.join([
                f"{k} : {v}" for k, v
                in summary.get('invalid_tests').items()
            ])
            file_handle.write(
                f"\nInvalid tests excluded from manifest:\n\t{invalid_tests}\n"
            )

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
        nicely formated path with leading and trailing forward slash
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
    print("Filling config file with reference files, before:")
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


def split_genepanels_test_codes(genepanels) -> pd.DataFrame:
    """
    Split out R/C codes from full CI name for easier matching
    against manifest

    +-----------------------+--------------------------+
    |      indication      |        panel_name        |
    +-----------------------+--------------------------+
    | C1.1_Inherited Stroke | CUH_Inherited Stroke_1.0 |
    | C2.1_INSR             | CUH_INSR_1.0             |
    +-----------------------+--------------------------+

                                    |
                                    ▼
                                        
    +-----------+-----------------------+---------------------------+
    | test_code |      indication      |        panel_name         |
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
    """
    genepanels['test_code'] = genepanels['indication'].apply(
        lambda x: x.split('_')[0] if re.match(r'[RC][\d]+\.[\d]+', x) else x
    )
    genepanels = genepanels[['test_code', 'indication', 'panel_name']]

    print(f"Genepanels file: \n{genepanels}")

    return genepanels


def parse_manifest(contents, split_tests=False) -> pd.DataFrame:
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

    Returns
    -------
    dict
        mapping of sampleID (str): 'tests': testCodes (list)
        e.g. {'sample1': {'tests': [['panel1']]}}
    str
        source of manifest file (either Epic or Gemini)
    
    Raises
    ------
    RuntimeError
        Raised when a sample seems malformed (missing / wrongly formatted IDs)
    RuntimeError
        Raised when file doesn't appear to have either ';' or '\t' as delimeter
    """
    print(
        "Parsing manifest file, file contents read from DNAnexus:\n\t",
        "\n\t".join(contents)
    )

    # turn manifest into a dict mapping sample ID to list of test codes,
    # duplicate samples in the same manifest for Epic samples will result
    # in >1 list of test codes , will be structured as:
    # {'sample1': {'tests': [['panel1', 'gene1'], ['panel2']]}}
    # for Gemini samples we will squash these down to a single list due
    # to how they are booked in and get split to multiple lines (it's going
    # away anyway so this is just for handling legacy samples)

    if all('\t' in x for x in contents if x):
        # this is an old Gemini manifest => should just have sampleID -> CI
        contents = [x.split('\t') for x in contents if x]

        # sense check data does only have 2 columns
        assert all([len(x) == 2 for x in contents]), (
            f"Gemini manifest has more than 2 columns:\n\t{contents}"
        )

        # initialise a dict of sample names to add tests to
        sample_names = {x[0] for x in contents}
        data = {name: {'tests': [[]]} for name in sample_names}

        for sample in contents:
            test_codes = sample[1].replace(' ', '').split(',')
            if not all([
                re.match(r"[RC][\d]+\.[\d]+|_HGNC:[\d]+", x) for x in test_codes
            ]):
                # TODO - as above, error or throw out
                raise RuntimeError(
                    'Invalid test code(s) provided for sample '
                    f'{sample[0]} : {sample[1]}'
                )
            # add test codes to samples list, keeping just the code part
            # and not full string (i.e. R134.2 from
            # R134.1_Familialhypercholesterolaemia_P)
            data[sample[0]]['tests'][0].extend([
                re.match(r"[RC][\d]+\.[\d]+|_HGNC:[\d]+", x).group()
                for x in test_codes
            ])

        manifest_source = 'Gemini'

    elif all(';' in x for x in contents[1:] if x):
        # csv file => Epic style manifest
        # (not actually a csv file even though they call it .csv since it
        # has ; as a delimeter and everything is a lie)
        # first row is just batch ID and 2nd is column names
        contents = [x.split(';') for x in contents if x]
        manifest = pd.DataFrame(contents[2:], columns=contents[1])

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
            'Re-analysis Specimen ID'
        ]

        # remove any spaces and SP- from specimen columns
        manifest[columns] = manifest[columns].applymap(
            lambda x: x.replace(' ', ''))
        manifest['Re-analysis Specimen ID'] = \
            manifest['Re-analysis Specimen ID'].str.replace('SP-', '')
        manifest['Specimen ID'] = \
            manifest['Specimen ID'].str.replace('SP-', '')

        # sample id may be split between 'Specimen ID' and 'Instrument ID' or
        # Re-analysis Specimen ID and Re-analysis Instrument ID columns, join
        # these as {InstrumentID-SpecimenID} to get a mapping of sample ID -> CI
        manifest['SampleID'] = manifest['Instrument ID'] + \
            '-' + manifest['Specimen ID']
        manifest['ReanalysisID'] = manifest['Re-analysis Instrument ID'] + \
            '-' + manifest['Re-analysis Specimen ID']

        manifest = manifest[['SampleID', 'ReanalysisID', 'Test Codes']]
        manifest_source = 'Epic'

        data = defaultdict(lambda: defaultdict(list))


        for idx, row in manifest.iterrows():
            # split test codes to list and sense check they're valid format
            # will be formatted as 'R211.1, , , ,' or '_HGNC:1234, , , ,' etc.
            test_codes = [
                x for x in row['Test Codes'].replace(' ', '').split(',') if x
            ]
            if not all([
                re.match(r"[RC][\d]+\.[\d]+|_HGNC", x) for x in test_codes
            ]):
                # TODO - do we want to raise an error here or just throw it out?
                raise RuntimeError(
                    f'Badly formatted test code provided for sample {row}'
                )

            # prefentially use ReanalysisID if present
            if re.match(r"[\d\w]+-[\d\w]+", row.ReanalysisID):
                data[row.ReanalysisID]['tests'].append(test_codes)
            elif re.match(r"[\d\w]+-[\d\w]+", row.SampleID):
                data[row.SampleID]['tests'].append(test_codes)
            else:
                # some funky with this sample naming
                raise RuntimeError(
                    f"Error in sample formatting of row {idx + 1} in manifest:"
                    f"\n\t{row}"
                )
    else:
        # throw an error here as something is up with the file
        raise RuntimeError("Manifest file provided does not seem valid")

    if split_tests:
        manifest = split_manifest_tests(manifest)

    samples = ('\n\t').join([
        f"{x[0]} -> {x[1]['tests']}" for x in data.items()
    ])
    print(f"{manifest_source} manifest parsed:\n\t{samples}")

    return data, manifest_source


def filter_manifest_samples_by_files(
        manifest, files, name, pattern) -> Tuple[dict, list, list]:
    """
    Filter samples in manifest against those where required per sample
    files have been found with DXManage.find_files().

    Used where there may be required per sample files missing for a given
    sample (i.e. .sample has failed or explicitly been excluded from running)

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
    print(f"Filtering manifest samples against available {name} files")
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


def check_manifest_valid_test_codes(manifest, genepanels) -> Tuple[dict, dict]:
    """
    Parse through manifest dict of sampleID -> test codes to check
    all codes are valid and exlcude those that are invalid against
    genepanels file

    Parameters
    ----------
    manifest : dict
        mapping of sampleID -> test codes
    genepanels : pd.DataFrame
        dataframe of genepanels file

    Returns
    -------
    Tuple[dict, dict]
        2 dicts of manifest with valid test codes and those that are invalid
    """
    print("Checking test codes in manifest are valid...")
    invalid = defaultdict(list)
    valid = defaultdict(lambda: defaultdict(list))

    genepanels_test_codes = sorted(set(genepanels['test_code'].tolist()))

    print(f"Current valid test codes:\n\t{genepanels_test_codes}")

    for sample, test_codes in manifest.items():
        sample_invalid_test = []

        # test codes stored under 'tests' key and is a list of lists
        # dependent on what genes / panels have been requested
        for test_list in test_codes['tests']:
            valid_tests = []
            for test in test_list:
                if test in genepanels_test_codes or re.match(r'_HGNC:[\d]+', test):
                    #TODO: should we check that we have a transcript assigned
                    # to this HGNC ID?
                    valid_tests.append(test)
                else:
                    sample_invalid_test.append(test)
            if valid_tests:
                # one or more requested test is in genepanels
                valid[sample]['tests'].append(list(set(valid_tests)))

        if sample_invalid_test:
            # sample had one or more invalid test code
            invalid[sample].extend(sample_invalid_test)

    if invalid:
        print(
            "WARNING: one or more samples had an invalid test "
            f"requested:\n\t{invalid}" 
        )
    else:
        print("All sample test codes valid!")

    # check if any samples only had test codes that are invalid -> won't
    # have any reports generated
    no_tests = set(manifest.keys()) - set(valid.keys())
    if no_tests:
        print(
            "WARNING: samples with invalid test codes resulting in having "
            f"no tests to run reports for: {no_tests}"
        )

    if not valid:
        raise RuntimeError(
            "All samples had invalid test codes resulting in an empty manifest"
        )

    return valid, invalid


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
        mapping of SampleID : [testCodes]

    Returns
    -------
    dict
        mapping of SampleID: 'tests': [testCodes] with all codes are sub lists
    """
    split_data = defaultdict(lambda: defaultdict(list))
    for sample, test_codes in data.items():
        all_split_test_codes = []
        for test_list in test_codes['tests']:
            test_genes = []
            for idx, sub_test in enumerate(test_list):
                if re.match(r"[RC][\d]+\.[\d]+", sub_test):
                    # it's a panel => split it out
                    all_split_test_codes.append([sub_test])
                else:
                    # it's a gene, add these back to a list to group
                    test_genes.append(sub_test)
            if test_genes:
                # there were some single genes to test
                all_split_test_codes.append(list(set(test_genes)))

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
    print("Finding panels and clinical indications for tests")
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
                    genepanels_row = genepanels[genepanels['test_code'] == test]

                    assert not genepanels_row.empty, (
                        f"Filtering genepanels for {test} returned empty df"
                    )

                    panels.append(genepanels_row.iloc[0].panel_name)
                    indications.append(genepanels_row.iloc[0].indication)
                elif re.fullmatch(r'_HGNC:[\d]+', test):
                    # add gene IDs as is to all lists
                    panels.append(test)
                    indications.append(test)
                else:
                    # we already validated earlier all the test codes so
                    # shouldn't get here
                    raise RuntimeError(
                        f"Error occured selecting testing from genepanels for "
                        f"test {test}"
                    )
            sample_tests['panels'].append(panels)
            sample_tests['indications'].append(indications)

        manifest_with_panels[sample] = sample_tests

    print("Manifest after")
    PPRINT(manifest_with_panels)

    return manifest_with_panels
