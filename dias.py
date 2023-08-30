#!/usr/bin/python

"""
Using an assay config (Python file), predefined commands call the relevant
workflows / apps defined in the config for the analysis of RD NGS samples.
Handles correctly interpreting and parsing inputs, defining output projects
and directory structures.
See READme for full documentation of how to structure the config file and what
inputs are valid.
"""

import argparse
import imp
import os

from general_functions import get_latest_config
from cnvcalling import run_cnvcall_app
from reports import run_reports
from cnvreports import run_cnvreports


ASSAY_OPTIONS = {
    "TSOE": ["egg1", "/mnt/storage/apps/software/egg1_dias_TSO_config"],
    "FH": ["egg3", "/mnt/storage/apps/software/egg3_dias_FH_config"],
    "TWE": ["egg4", "/mnt/storage/apps/software/egg4_dias_TWE_config"],
    "CEN": ["egg5", "/mnt/storage/apps/software/egg5_dias_CEN_config"]
}


def parse_CLI_args(): # -> argparse.Namespace:
    """
    Parse command line arguments
    -------
    Returns
    args : Namespace
        Namespace of passed command line argument inputs
    """
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    parser.add_argument(
        "-d", "--dry_run", action="store_true",
        default=False, help="Make a dry run"
    )
    parser.add_argument(
        "-a", "--assay", choices=ASSAY_OPTIONS.keys(), help=(
            "Type of assay needed for this run of samples"
        )
    )
    parser.add_argument(
        "-c", "--config", help="Config file to overwrite the assay setup"
    )

    # Parsing command line args for run-level CNV calling
    parser_n = subparsers.add_parser('cnvcall', help='cnvcall help')
    parser_n.add_argument(
        'input_dir', type=str,
        help='A single sample workflow output directory path'
    )
    parser_n.add_argument(
        'sample_list', type=str, nargs="?",
        help=(
            'File containing samples that should be EXCLUDED from CNV analysis'
            '. One sample name per line'
        )
    )
    parser_n.set_defaults(which='cnvcall')

    # Parsing command line args for SNV reports
    parser_r = subparsers.add_parser('reports', help='reports help')
    parser_r.add_argument(
        'input_dir', type=str,
        help='A single sample workflow output directory path'
    )
    parser_r.add_argument(
        'sample_ID_TestCode', type=str, nargs="?",
        help=(
            'DNAnexus file-ID of a csv file containing samples and '
            ' clinical indications for SNV report generation'
            '. One sample name per row with semicolon separated R codes'
        )
    )
    parser_r.set_defaults(which='reports')

    # Parsing command line args for Mosaic reports
    parser_r = subparsers.add_parser('mosaicreports', help='mosaicreports help')
    parser_r.add_argument(
        'input_dir', type=str,
        help='A single sample workflow output directory path'
    )
    parser_r.add_argument(
        'sample_ID_TestCode', type=str, nargs="?",
        help=(
            'DNAnexus file-ID of a csv file containing samples and '
            ' clinical indications for Mosaic report generation'
            '. One sample name per row with semicolon separated R codes'
        )
    )
    parser_r.set_defaults(which='mosaicreports')

    # Parsing command line args for SNV reanalysis
    parser_r = subparsers.add_parser('reanalysis', help='reanalysis help')
    parser_r.add_argument(
        'input_dir', type=str,
        help='A single sample workflow output directory path'
    )
    parser_r.add_argument(
        'sample_X_CI', type=str,
        help=(
            'Tab delimited file containing sample and panel for reanalysis'
            '. One sample/panel combination per line'
        )
    )
    parser_r.set_defaults(which='reanalysis')

    # Parsing command line args for CNV reports
    parser_cr = subparsers.add_parser('cnvreports', help='cnvreports help')
    parser_cr.add_argument(
        'input_dir', type=str,
        help='A CNV calling output directory path'
    )
    parser_cr.add_argument(
        'cnvsample_ID_TestCode', type=str, nargs="?",
        help=(
            'DNAnexus file-ID of a csv file containing samples and '
            ' clinical indications for CNV report generation'
            '. One sample name per row with semicolon separated R codes'
        )
    )
    parser_cr.set_defaults(which='cnvreports')

    # Parsing command line args for CNV reanalysis
    parser_cr = subparsers.add_parser('cnvreanalysis', help='cnvreanalysis help')
    parser_cr.add_argument(
        'input_dir', type=str,
        help='A CNV calling output directory path'
    )
    parser_cr.add_argument(
        'cnvsample_X_CI', type=str,
        help=(
            'Tab delimited file containing sample and panel for cnvreanalysis'
            '. One sample/panel combination per line'
        )
    )
    parser_cr.set_defaults(which='cnvreanalysis')

    args = parser.parse_args()
    return args


def load_assay_config(assay):
    """Simple function to locate and load the latest version of assay config

    Args:
        assay (str): name of the assay to load latest version of config file

    Returns:
        config: info parsed from assay config file
    """
    # look up the config folder path for the selected assay
    config_folder_path = ASSAY_OPTIONS[assay][1]
    # identify the latest version available
    latest_version = get_latest_config(config_folder_path)
    # look up the EGG code of the assay
    assay_code = ASSAY_OPTIONS[assay][0]
    # locate the assay config file with or without version in the filename
    try:
        config_filename = "".join([assay_code + "_config_v" + latest_version + ".py"])
        config_path = os.path.join(config_folder_path, latest_version, config_filename)
        os.path.exists(config_path) is True
    except:
        config_filename = "".join([assay_code + "_config.py"])
        config_path = os.path.join(config_folder_path, latest_version, config_filename)

    config = imp.load_source(config_filename, config_path)
    return config


def main():
    """Main entry point to set off app/workflow based on subcommand
    with specified config
    """
    args = parse_CLI_args()

    # Locate and load correct config file
    # config file may be specified as custom or as the latest for valid assays
    assert args.assay or args.config, ("Please specify either a valid assay name " \
        "with -a, or path to a custom config file with -c")
    if args.config:
        config_filename = os.path.splitext(args.config)[0]
        config = imp.load_source(config_filename, args.config)
        assay_id = "_".join(["CUSTOM_CONFIG", config.assay_name, config.assay_version])
    else:
        config = load_assay_config(args.assay)
        assay_id = "_".join([config.assay_name, config.assay_version])

    # Ensure that a DNAnexus path to collect input files from is specified
    assert args.input_dir, "Please specify a DNAnexus input directory"
    # Ensure that DNAnexus path has trailing forward slash
    if not args.input_dir.endswith("/"):
        args.input_dir = args.input_dir + "/"

    # Prepare to run appropriate workflow as specified by the valid subcommand
    subcommand = args.which

    # Set off relevant workflow based on subcommand with provided inputs
    if subcommand == "cnvcall":
        assert args.sample_list, "Please specify a sample exclusion list, including at least the control sample"
        cnvcall_applet_out_dir = run_cnvcall_app(
            args.input_dir, args.dry_run, config, assay_id,
            args.sample_list
        )
    elif subcommand == "reports":
        reports_out_dir = run_reports(
            args.input_dir, args.dry_run, config, assay_id,
            sample_ID_TestCode = args.sample_ID_TestCode
        )
    elif subcommand == "mosaicreports":
        reports_out_dir = run_reports(
            args.input_dir, args.dry_run, True, config, assay_id,
            sample_ID_TestCode = args.sample_ID_TestCode
        )
    elif subcommand == "reanalysis":
        reports_out_dir = run_reports(
            args.input_dir, args.dry_run, config, assay_id,
            sample_X_CI = args.sample_X_CI
        )
    elif subcommand == "cnvreports":
        cnvreports_out_dir = run_cnvreports(
            args.input_dir, args.dry_run, config, assay_id,
            sample_ID_TestCode = args.cnvsample_ID_TestCode
        )
    elif subcommand == "cnvreanalysis":
        cnv_reports_out_dir = run_cnvreports(
            args.input_dir, args.dry_run, config, assay_id,
            sample_X_CI = args.cnvsample_X_CI
        )
    else:
        print("Please specify a valid subcommand from: 'cnvcall', 'reports', "
            "'reanalysis', 'cnvreports', 'cnvreanalysis'")


if __name__ == "__main__":
    main()
