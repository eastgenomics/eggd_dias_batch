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


def parse_manifest(contents) -> pd.DataFrame:
    """
    Parse manifest data from file read in DNAnexus

    Handles both Gemini and Epic style manifest files

    Parameters
    ----------
    contents : list
        manifest file data

    Returns
    -------
    pd.DataFrame
        manifest as df
    
    Raises
    ------
    RuntimeError
        Raised when file doesn't appear to have either ';' or '\t' as delimeter
    """
    print(
        "Parsing manifest file, file contents read from DNAnexus:\n\t",
        "\n\t".join(contents)
    )

    if all(';' in x for x in contents[1:] if x):
        # csv file => Epic style manifest
        # (not actually a csv file even though they call it .csv since it
        # has ; as a delimeter and everything is a lie)
        # first row is just batch ID and 2nd is column names
        contents = [x.split(';') for x in contents if x]
        manifest = pd.DataFrame(contents[2:], columns=contents[1])

        # sample id may be split between 'Sepcimen ID' and 'Instrument ID' or
        # Re-analysis Specimen ID and Re-analysis Instrument ID olumns, join
        # these as {InstrumentID-SpecimenID} to get a mapping of sample ID -> CI
        manifest['SampleID'] = manifest['Instrument ID'] + '-' + manifest['Specimen ID']
        manifest['ReanalysisID'] = manifest['Re-analysis Instrument ID'] + \
            '-' + manifest['Re-analysis Specimen ID']
        
        manifest = manifest[['SampleID', 'ReanalysisID', 'Test Codes']]

        # turn test codes into list where there are multiple, will
        # be formatted as 'R211.1, , , ,' or '_HGNC:1234, , , ,'
        manifest['Test Codes'] = manifest['Test Codes'].apply(
            lambda codes: [
                x for x in codes.replace(' ', '').split(',')
                if re.match(r"[RC][\d]+\.[\d]+|_HGNC", x)
            ]
        )

        #TODO - figure out logic of handling normal and reanalysis and what to throw out

        manifest.name = 'Epic'

    elif all('\t' in x for x in contents if x):
        # this is an old Gemini manifest => should just have sampleID - CI
        contents = [x.split('\t') for x in contents if x]

        # sense check data does only have 2 columns
        assert all([len(x)==2 for x in contents]), (
            f"Gemini manifest has more than 2 columns:\n\t{contents}"
        )

        manifest = pd.DataFrame(contents, columns=['SampleID', 'CI'])

        # can be multiple test codes as comma separated string => turn
        # these into a nicely formatted list
        manifest['CI']= manifest['CI'].apply(
            lambda codes: [
                x for x in codes.replace(' ', '').split(',')
                if re.match(r"[RC][\d]+\.[\d]+|_HGNC", x)
            ]
        )

        manifest.name = 'Gemini'
    else:
        # throw an error here as something is up with the file
        raise RuntimeError(
            f"Manifest file provided does not seem valid"
        )
    
    print(f"{manifest.name} manifest parsed into dataframe:\n\t{manifest}")

    return manifest
