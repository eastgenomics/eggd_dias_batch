"""
Functions for filling in assay config reference files and dynamic inputs
"""
from copy import deepcopy
import re

from .formatting import prettier_print


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
    print("\n \nFilling config file with reference files...")

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


def add_dynamic_inputs(config, **kwargs) -> dict:
    """
    Adds the given value in place of the placeholder text from assay config

    `kwargs` input are expected to be a mapping of the placeholder as
    defined in the config without the INPUT- prefix and the input to
    add in to the config

    Parameters
    ----------
    config : dict
        config with input placeholders to replace
    kwargs : dict
        mapping of placeholders to replace and replacement values

    Returns
    -------
    dict
        config with filled placeholders
    """
    filled_config = {}

    for field, config_value in config.items():
        if isinstance(config_value, str):
            if kwargs.get(config_value.replace('INPUT-', '')):
                config_value = kwargs.get(config_value.replace('INPUT-', ''))

        filled_config[field] = config_value

    # sense check we removed all placeholders
    if any([
        x.startswith('INPUT-') if isinstance(x, str) else False
        for x in filled_config.values()
    ]):
        print(
            "Filled config still has placeholders in it, this likely means "
            "one or more required inputs were not provided to the script, "
            "please check the config and your inputs:"
        )
        prettier_print(filled_config)
    assert not any([
        x.startswith('INPUT-') if isinstance(x, str) else False
        for x in filled_config.values()
    ]), "INPUT- placeholders left in config"

    return filled_config
