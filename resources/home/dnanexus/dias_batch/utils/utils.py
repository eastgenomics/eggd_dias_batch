from collections import defaultdict
from datetime import datetime
import re

import pandas as pd


def time_stamp() -> str:
    """
    Returns string of date & time formatted as YYMMDD_HHMM

    Returns
    -------
    str
        String of current date and time as YYMMDD_HHMM
    """
    return datetime.now().strftime("%y%m%d_%H%M")


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

    # turn df into a dict mapping sample ID to list of test codes,
    # duplicate samples in the same manifest will result in >1 list
    # of test codes, will be structured as:
    # {'sample1': {'tests': [['panel1', 'gene1'], ['panel2']]}}
    data = defaultdict(lambda: defaultdict(list))

    if all(';' in x for x in contents[1:] if x):
        # csv file => Epic style manifest
        # (not actually a csv file even though they call it .csv since it
        # has ; as a delimeter and everything is a lie)
        # first row is just batch ID and 2nd is column names
        contents = [x.split(';') for x in contents if x]
        manifest = pd.DataFrame(contents[2:], columns=contents[1])

        # sample id may be split between 'Specimen ID' and 'Instrument ID' or
        # Re-analysis Specimen ID and Re-analysis Instrument ID columns, join
        # these as {InstrumentID-SpecimenID} to get a mapping of sample ID -> CI
        manifest['SampleID'] = manifest['Instrument ID'] + \
            '-' + manifest['Specimen ID']
        manifest['ReanalysisID'] = manifest['Re-analysis Instrument ID'] + \
            '-' + manifest['Re-analysis Specimen ID']
        
        manifest = manifest[['SampleID', 'ReanalysisID', 'Test Codes']]
        manifest_source = 'Epic'

        for idx, row in manifest.iterrows():
            # split test codes to list and sense check they're valid format
            # will be formatted as 'R211.1, , , ,' or '_HGNC:1234, , , ,' etc.
            test_codes = [
                x for x in row['Test Codes'].replace(' ', '').split(',') if x
            ]
            if not all([
                re.match(r"[RC][\d]+\.[\d]+|_HGNC", x) for x in test_codes
            ]):
                raise RuntimeError(
                    f'Invalid test code provided for sample {row}'
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

    elif all('\t' in x for x in contents if x):
        # this is an old Gemini manifest => should just have sampleID -> CI
        contents = [x.split('\t') for x in contents if x]

        # sense check data does only have 2 columns
        assert all([len(x)==2 for x in contents]), (
            f"Gemini manifest has more than 2 columns:\n\t{contents}"
        )

        data = defaultdict(list)
        for sample in contents:
            test_codes = sample[1].replace(' ', '').split(',')
            if not all([
                re.match(r"[RC][\d]+\.[\d]+|_HGNC", x) for x in test_codes
            ]):
                raise RuntimeError(
                    'Invalid test code provided for sample '
                    f'{sample[0]} : {sample[1]}'
                )
            data[sample[0]]['tests'].append(sample[1])
        
        manifest_source = 'Gemini'

    else:
        # throw an error here as something is up with the file
        raise RuntimeError(
            f"Manifest file provided does not seem valid"
        )

    samples = ('\n\t').join([
        f"{x[0]} -> {x[1]['tests']}" for x in data.items()
    ])
    print(f"{manifest_source} manifest parsed:\n\t{samples}")

    return data


def filter_manifest_samples_by_files(manifest, files, pattern) -> dict:
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
    pattern : str
        regex pattern for selecting parts of name to match on, i.e.
            (Gemini naming)
            manifest name : X12345
            vcf name      : X12345-GM12345_much_suffix.vcf.gz
            pattern       : '^X[\d]+'

            (Epic naming)
            manifest_name : 124801362-23230R0131
            vcf name      : 124801362-23230R0131-23NGSCEN15-8128-M-96527.vcf.gz
            pattern       : '^[\d\w]+-[\d\w]+'


    Returns
    -------
    dict
        subset of manifest mapping dict with samples removed that have
        no files and with DXFile objects added under 'files' as a list
        for each sample where one or more files were found
    """
    # build mapping of prefix using given pattern to matching files
    file_prefixes = defaultdict(list)
    for file in files:
        match = re.match(pattern, file['describe']['name'])
        if match:
            file_prefixes[match.group()].append(file)

    manifest_no_match = []
    manifest_no_files = []

    for sample in manifest.keys():
        match = re.match(pattern, sample)
        if not match:
            # sample ID doesn't match expected pattern
            manifest_no_match.append(sample)
            manifest.pop(sample)
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
                manifest.pop(sample)
            else:
                print(
                    f"Found {len(sample_files)} files for {sample}\n"
                    f"{[x['describe']['name'] for x in sample_files]}"
                )
                manifest[sample]['files'] = sample_files
    
    print(
        f"{len(manifest_no_match)} samples in manifest didn't match expected"
        f"pattern of {pattern}: {manifest_no_match}"
    )

    print(
        f"{len(manifest_no_files)} samples in manifest didn't have any "
        f"matching files: {manifest_no_files}"
    )

    return manifest



                




    manifest_mapping = {
        re.match(pattern, k).group(): k
        if re.match(pattern, k) else None
        for k, v in manifest
    }



def split_tests(data) -> dict:
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
                    all_split_test_codes.append([test])
                else:
                    # it's a gene, add these back to a list to group
                    test_genes.append(sub_test)
            if test_genes:
                # there were some single genes to test
                all_split_test_codes.append(list(set(test_genes)))
        
        split_data[sample]['tests'].extend(all_split_test_codes)
    
    return split_data


