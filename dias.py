#!/usr/bin/python

import argparse

from single_workflow import run_ss_workflow
from multi_workflow import run_ms_workflow
from multiqc import run_multiqc_app
from vcf2xls import run_vcf2xls_app, run_reanalysis


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

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
    parser_r.set_defaults(which='reports')

    parser_r = subparsers.add_parser('reanalysis', help='reanalysis help')
    parser_r.add_argument(
        'input_dir', type=str,
        help='A multi sample workflow output directory path'
    )
    parser_r.add_argument(
        'reanalysis_list', type=str,
        help=(
            'Tab delimited file containg sample and panel for reanalysis'
            '. One sample/panel combination per line'
        )
    )
    parser_r.set_defaults(which='reanalysis')

    args = parser.parse_args()
    workflow = args.which

    assert workflow, "Please specify a subcommand"

    if args.input_dir and not args.input_dir.endswith("/"):
        args.input_dir = args.input_dir + "/"

    if workflow == "single":
        ss_workflow_out_dir = run_ss_workflow(args.input_dir)
    elif workflow == "multi":
        ms_workflow_out_dir = run_ms_workflow(args.input_dir)
    elif workflow == "qc":
        mqc_applet_out_dir = run_multiqc_app(args.input_dir)
    elif workflow == "reports":
        reports_out_dir = run_vcf2xls_app(args.input_dir)
    elif workflow == "reanalysis":
        reports_out_dir = run_reanalysis(args.input_dir, args.reanalysis_list)


if __name__ == "__main__":
    main()
