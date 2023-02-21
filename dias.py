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
from single_workflow import run_ss_workflow
from multi_workflow import run_ms_workflow
from multiqc import run_multiqc_app
from cnvcalling import run_cnvcall_app
from reports import run_reports, run_reanalysis
from cnvreports import run_cnvreports, run_cnvreanalysis


ASSAY_OPTIONS = {
    "TSOE": ["egg1", "/mnt/storage/apps/software/egg1_dias_TSO_config"],
    "FH": ["egg", "/mnt/storage/apps/software/egg3_dias_FH_config"],
    "TWE": ["egg4", "/mnt/storage/apps/software/egg4_dias_TWE_config"],
    # "CEN": ["egg5", "/mnt/storage/apps/software/egg5_dias_CEN_config"]
    "CEN": ["egg5", "/home/sophier/Documents/work/prog/DNAnexus/egg5_dias_CEN_config"]
}


def load_assay_config(assay):
    """Simple function to locate and load the latest version of assay config

    Args:
        assay (str): name of the assay to load latest version of config file

    Returns:
        config: info parsed from assay config file
    """
    # ilook up the config folder path for the selected assay
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

    # Parsing command line args for single sample workflow
    parser_s = subparsers.add_parser('single', help='single help')
    parser_s.add_argument(
        'input_dir', type=str, help='A sequencing data (FASTQ) directory path'
    )
    parser_s.set_defaults(which='single')

    # Parsing command line args for multi sample workflow
    parser_m = subparsers.add_parser('multi', help='multi help')
    parser_m.add_argument(
        'input_dir', type=str,
        help='A single sample workflow output directory path'
    )
    parser_m.set_defaults(which='multi')

    # Parsing command line args for run QC
    parser_q = subparsers.add_parser('qc', help='multiqc help')
    parser_q.add_argument(
        'input_dir', type=str,
        help='A multi sample workflow output directory path'
    )
    parser_q.set_defaults(which='qc')

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
    parser_r = subparsers.add_parser('cnvreports', help='cnvreports help')
    parser_r.add_argument(
        'input_dir', type=str,
        help='A CNV calling output directory path'
    )
    parser_r.set_defaults(which='cnvreports')

    # Parsing command line args for CNV reanalysis
    parser_r = subparsers.add_parser('cnvreanalysis', help='cnvreanalysis help')
    parser_r.add_argument(
        'input_dir', type=str,
        help='A CNV calling output directory path'
    )
    parser_r.add_argument(
        'cnvreanalysis_list', type=str,
        help=(
            'Tab delimited file containing sample and panel for cnvreanalysis'
            '. One sample/panel combination per line'
        )
    )
    parser_r.set_defaults(which='cnvreanalysis')

    args = parser.parse_args()
    workflow = args.which

    assert workflow, "Please specify a subcommand"

    if args.config:
        assay_id = "CUSTOM_CONFIG"
        name_config = os.path.splitext(args.config)[0]
        config = imp.load_source(name_config, args.config)
    else:
        config = load_assay_config(args.assay)
        assay_id = "{}_{}".format(config.assay_name, config.assay_version)

    if args.input_dir and not args.input_dir.endswith("/"):
        args.input_dir = args.input_dir + "/"

    if workflow == "single":
        ss_workflow_out_dir = run_ss_workflow(
            args.input_dir, args.dry_run, config, assay_id
        )
    elif workflow == "multi":
        ms_workflow_out_dir = run_ms_workflow(
            args.input_dir, args.dry_run, config, assay_id
        )
    elif workflow == "qc":
        mqc_applet_out_dir = run_multiqc_app(
            args.input_dir, args.dry_run, config, assay_id
        )
    elif workflow == "cnvcall":
        cnvcall_applet_out_dir = run_cnvcall_app(
            args.input_dir, args.dry_run, config, assay_id,
            args.sample_list
        )
    elif workflow == "cnvreports":
        cnv_reports_out_dir = run_cnvreports(
            args.input_dir, args.dry_run, config, assay_id
        )
    elif workflow == "cnvreanalysis":
        cnv_reports_out_dir = run_cnvreanalysis(
            args.input_dir, args.dry_run, config, assay_id,
            args.cnvreanalysis_list
        )
    elif workflow == "reports":
        reports_out_dir = run_reports(
            args.input_dir, args.dry_run, config, assay_id
        )
    elif workflow == "reanalysis":
        reports_out_dir = run_reanalysis(
            args.input_dir, args.dry_run, config, assay_id,
            args.reanalysis_list
        )


if __name__ == "__main__":
    main()
