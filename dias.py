#!/usr/bin/python

import argparse
import imp
import os

from single_workflow import run_ss_workflow
from multi_workflow import run_ms_workflow
from multiqc import run_multiqc_app
from cnvcalling import run_cnvcall_app
from cnvreports import run_cnvreports, run_cnvreanalysis
from reports import run_reports, run_reanalysis
from general_functions import get_latest_config


TSOE_CONFIG_LOCATION = "/mnt/storage/apps/software/egg1_dias_TSO_config"
FH_CONFIG_LOCATION = "/mnt/storage/apps/software/egg3_dias_FH_config"
TWE_CONFIG_LOCATION = "/mnt/storage/apps/software/egg4_dias_TWE_config"
CEN_CONFIG_LOCATION = "/mnt/storage/apps/software/egg5_dias_CEN_config"


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    parser.add_argument(
        "-d", "--dry_run", action="store_true",
        default=False, help="Make a dry run"
    )

    parser.add_argument(
        "-a", "--assay", choices=["TSOE", "FH", "TWE", "CEN"], help=(
            "Type of assay needed for this run of samples"
        )
    )
    parser.add_argument(
        "-c", "--config", help="Config file to overwrite the config assay setup"
    )

    parser_s = subparsers.add_parser('single', help='single help')
    parser_s.add_argument(
        'input_dir', type=str, help='Input data directory path'
    )
    parser_s.set_defaults(which='single')

    parser_m = subparsers.add_parser('multi', help='multi help')
    parser_m.add_argument(
        'input_dir', type=str,
        help='A single sample workflow output directory path'
    )
    parser_m.set_defaults(which='multi')

    parser_q = subparsers.add_parser('qc', help='multiqc help')
    parser_q.add_argument(
        'input_dir', type=str,
        help='A single/multi sample workflow output directory path'
    )
    parser_q.set_defaults(which='qc')

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

    parser_r = subparsers.add_parser('cnvreports', help='cnvreports help')
    parser_r.add_argument(
        'input_dir', type=str,
        help='A single/multi sample workflow output directory path'
    )
    parser_r.set_defaults(which='cnvreports')

    parser_r = subparsers.add_parser('cnvreanalysis', help='cnvreanalysis help')
    parser_r.add_argument(
        'input_dir', type=str,
        help='A single/multi sample workflow output directory path'
    )
    parser_r.add_argument(
        'cnvreanalysis_list', type=str,
        help=(
            'Tab delimited file containing sample and panel for cnvreanalysis'
            '. One sample/panel combination per line'
        )
    )
    parser_r.set_defaults(which='cnvreanalysis')

    parser_r = subparsers.add_parser('reports', help='reports help')
    parser_r.add_argument(
        'input_dir', type=str,
        help='A single/multi sample workflow output directory path'
    )
    parser_r.set_defaults(which='reports')

    parser_r = subparsers.add_parser('reanalysis', help='reanalysis help')
    parser_r.add_argument(
        'input_dir', type=str,
        help='A single/multi sample workflow output directory path'
    )
    parser_r.add_argument(
        'reanalysis_list', type=str,
        help=(
            'Tab delimited file containing sample and panel for reanalysis'
            '. One sample/panel combination per line'
        )
    )
    parser_r.set_defaults(which='reanalysis')

    args = parser.parse_args()
    workflow = args.which

    assert workflow, "Please specify a subcommand"

    if args.config:
        assay_id = "CUSTOM_CONFIG"
        name_config = os.path.splitext(args.config)[0]
        config = imp.load_source(name_config, args.config)
    else:
        if args.assay == "TSOE":
            latest_version = get_latest_config(TSOE_CONFIG_LOCATION)
            config = imp.load_source(
                "egg1_config", "{}/{}/egg1_config.py".format(
                    TSOE_CONFIG_LOCATION, latest_version
                )
            )
        elif args.assay == "FH":
            latest_version = get_latest_config(FH_CONFIG_LOCATION)
            config = imp.load_source(
                "egg3_config", "{}/{}/egg3_config.py".format(
                    FH_CONFIG_LOCATION, latest_version
                )
            )
        elif args.assay == "TWE":
            latest_version = get_latest_config(TWE_CONFIG_LOCATION)
            config = imp.load_source(
                "egg4_config", "{}/{}/egg4_config.py".format(
                    TWE_CONFIG_LOCATION, latest_version
                )
            )
        elif args.assay == "CEN":
            latest_version = get_latest_config(CEN_CONFIG_LOCATION)
            config = imp.load_source(
                "egg5_config", "{}/{}/egg5_config.py".format(
                    CEN_CONFIG_LOCATION, latest_version
                )
            )
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
