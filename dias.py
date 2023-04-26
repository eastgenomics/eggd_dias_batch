#!/usr/bin/python

"""
Using an assay config (in Python), predefined commands call the relevant
workflows / apps defined in the config for specified samples.
Handles correctly interpreting and parsing inputs, defining output projects
and directory structures.
See READme for full documentation of how to structure the config file and what
inputs are valid.
"""

import argparse
import imp
import os

from general_functions import get_latest_config
# from single_workflow import run_ss_workflow
# from multi_workflow import run_ms_workflow
# from multiqc import run_multiqc_app
from cnvcalling import run_cnvcall_app
from reports import run_reports, run_reanalysis
from cnvreports import run_cnvreports, run_cnvreanalysis


ASSAY_OPTIONS = {
    "TSOE": ["egg1", "/mnt/storage/apps/software/egg1_dias_TSO_config"],
    "FH": ["egg3", "/mnt/storage/apps/software/egg3_dias_FH_config"],
    "TWE": ["egg4", "/mnt/storage/apps/software/egg4_dias_TWE_config"],
    "CEN": ["egg5", "/mnt/storage/apps/software/egg5_dias_CEN_config"]
}


SUBCOMMAND_OPTIONS = {
    # "single": run_ss_workflow,
    # "multi": run_ms_workflow,
    # "qc": run_multiqc_app,
    "cnvcall": run_cnvcall_app,
    "reports": run_reports,
    "reanalysis": run_reanalysis,
    "cnvreports": run_cnvreports,
    "cnvreanalysis": run_cnvreanalysis
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

    # # Parsing command line args for single sample workflow
    # parser_s = subparsers.add_parser('single', help='single help')
    # parser_s.add_argument(
    #     'input_dir', type=str, help='A sequencing data (FASTQ) directory path'
    # )
    # parser_s.set_defaults(which='single')

    # # Parsing command line args for multi sample workflow
    # parser_m = subparsers.add_parser('multi', help='multi help')
    # parser_m.add_argument(
    #     'input_dir', type=str,
    #     help='A single sample workflow output directory path'
    # )
    # parser_m.set_defaults(which='multi')

    # # Parsing command line args for run QC
    # parser_q = subparsers.add_parser('qc', help='multiqc help')
    # parser_q.add_argument(
    #     'input_dir', type=str,
    #     help='A multi sample workflow output directory path'
    # )
    # parser_q.set_defaults(which='qc')

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
        'sample_panel', type=str, nargs="?",
        help=(
            'DNAnexus file-ID of a csv file containing samples and '
            ' clinical indications for SNV report generation'
            '. One sample name per row with semicolon separated R codes'
        )
    )
    parser_r.set_defaults(which='reports')

    # Parsing command line args for SNV reanalysis
    parser_r = subparsers.add_parser('reanalysis', help='reanalysis help')
    parser_r.add_argument(
        'input_dir', type=str,
        help='A single sample workflow output directory path'
    )
    parser_r.add_argument(
        'reanalysis_list', type=str,
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
        'sample_panel', type=str, nargs="?",
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
        'cnvreanalysis_list', type=str,
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

    # Prepare to run appropriate workflow as specified by the valid subcommand
    subcommand = args.which
    assert subcommand, "Please specify a valid subcommand {}".format(
        SUBCOMMAND_OPTIONS.keys()
    )
    # Ensure that a DNAnexus path to collect input files from is specified
    assert args.input_dir, "Please specify a DNAnexus input directory"
    # Ensure that DNAnexus path has trailing forward slash
    if not args.input_dir.endswith("/"):
        args.input_dir = args.input_dir + "/"

    # Set off relevant workflow based on subcommand
    # with applicable inputs
    if subcommand == "cnvcall":
        assert args.sample_list, "Please specify a sample exclusion list, including at least the control sample"
        cnvcall_applet_out_dir = run_cnvcall_app(
            args.input_dir, args.dry_run, config, assay_id,
            args.sample_list
        )
    elif subcommand == "reports":
        reports_out_dir = run_reports(
            args.input_dir, args.dry_run, config, assay_id,
            args.sample_panel
        )
    elif subcommand == "reanalysis":
        reports_out_dir = run_reanalysis(
            args.input_dir, args.dry_run, config, assay_id,
            args.reanalysis_list
        )
    elif subcommand == "cnvreports":
        cnvreports_out_dir = run_cnvreports(
            args.input_dir, args.dry_run, config, assay_id,
            args.sample_panel
        )
    elif subcommand == "cnvreanalysis":
        cnv_reports_out_dir = run_cnvreanalysis(
            args.input_dir, args.dry_run, config, assay_id,
            args.cnvreanalysis_list
        )
    else:
        SUBCOMMAND_OPTIONS[subcommand](
            args.input_dir, args.dry_run, config, assay_id
        )


if __name__ == "__main__":
    main()
