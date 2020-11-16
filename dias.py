#!/usr/bin/python

import argparse

from single_workflow import run_ss_workflow
from multi_workflow import run_ms_workflow
from multiqc import run_multiqc_app
from reports import run_reports, run_reanalysis
from check import check_if_all_reports_created


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    parser.add_argument(
        "-d", "--dry_run", action="store_true",
        default=False, help="Make a dry run"
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
        help='A multi sample workflow output directory path'
    )
    parser_q.set_defaults(which='qc')

    parser_r = subparsers.add_parser('reports', help='reports help')
    parser_r.add_argument(
        'input_dir', type=str,
        help='A multi sample workflow output directory path'
    )
    parser_r.add_argument(
        'sample_sheet', type=str, help="Path to the sample sheet"
    )
    parser_r.set_defaults(which='reports')

    parser_r = subparsers.add_parser('reanalysis', help='reanalysis help')
    parser_r.add_argument(
        'input_dir', type=str,
        help='A multi sample workflow output directory path'
    )
    parser_r.add_argument(
        'reanalysis_list', type=str,
        help=(
            'Tab delimited file containing sample and panel for reanalysis'
            '. One sample/panel combination per line'
        )
    )
    parser_r.set_defaults(which='reanalysis')

    parser_c = subparsers.add_parser("check", help="check help")
    parser_c.add_argument(
        "input_dir", type=str,
        help="A vcf2xls output directory path"
    )
    parser_c.add_argument(
        "sample_sheet", type=str,
        help="Path to the sample sheet"
    )
    parser_c.set_defaults(which="check_reports")

    args = parser.parse_args()
    workflow = args.which

    assert workflow, "Please specify a subcommand"

    if args.input_dir and not args.input_dir.endswith("/"):
        args.input_dir = args.input_dir + "/"

    if workflow == "single":
        ss_workflow_out_dir = run_ss_workflow(args.input_dir, args.dry_run)
    elif workflow == "multi":
        ms_workflow_out_dir = run_ms_workflow(args.input_dir, args.dry_run)
    elif workflow == "qc":
        mqc_applet_out_dir = run_multiqc_app(
            args.input_dir, args.config_multiqc, args.dry_run
        )
    elif workflow == "reports":
        reports_out_dir = run_reports(
            args.input_dir, args.dry_run, args.sample_sheet
        )
    elif workflow == "reanalysis":
        reports_out_dir = run_reanalysis(
            args.input_dir, args.dry_run, args.reanalysis_list
        )
    elif workflow == "check_reports":
        reports = check_if_all_reports_created(
            args.input_dir, args.sample_sheet
        )

        if reports:
            print("{} have not been generated".format(", ".join(reports)))
        else:
            print("All reports have been generated")


if __name__ == "__main__":
    main()
