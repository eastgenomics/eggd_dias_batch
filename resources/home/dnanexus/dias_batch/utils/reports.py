"""
Functions for building and writing the batch summary report
"""
import re
from time import strftime, localtime

import pandas as pd


# for prettier viewing in the logs
pd.set_option('display.max_rows', 200)
pd.set_option('max_colwidth', 1500)


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
            if re.search(r'[\d]{1,2}.xlsx$', x)
        ]

        if suffixes:
            # found something useful, if not we're just going to use 1
            suffix = max([
                int(x.group().replace('.xlsx', '')) for x in suffixes if x
            ])

    return suffix + 1


def write_summary_report(output, job, app, manifest=None, **summary) -> None:
    """
    Write output summary file with jobs launched and any errors etc.

    Parameters
    ----------
    output : str
        name for output file
    job : dict
        details from dxpy.describe() call on job ID
    app : dict
        details from dxpy.describe() call on app ID
    manifest : dict
        mapping of samples in manifest -> requested test codes
    summary : kwargs
        all possible named summary metrics to write

    Outputs
    -------
    {output}.txt file of launched job summary
    """
    print(f"\n \nWriting summary report to {output}")

    time = strftime('%Y-%m-%d %H:%M:%S', localtime(job['created'] / 1000))
    inputs = job['runInput']
    inputs = "\n\t".join([f"{x[0]}: {x[1]}" for x in sorted(inputs.items())])

    with open(output, 'w') as file_handle:
        file_handle.write(
            f"Jobs launched from {app.get('name')} ({app.get('version')}) at {time} "
            f"by {job['launchedBy'].replace('user-', '')} in {job['id']}\n"
        )

        file_handle.write(
            f"\nAssay config file used {summary.get('assay_config')['name']} "
            f"({summary.get('assay_config')['dxid']})\n"
        )

        file_handle.write(f"\nJob inputs:\n\t{inputs}\n")

        if manifest:
            file_handle.write(
                f"\nManifest(s) parsed: {job['runInput']['manifest_files']}\n"
            )
            file_handle.write(
                "\nTotal number of samples in provided manifest(s): "
                f"{len(summary.get('provided_manifest_samples'))}"
            )
            file_handle.write(
                f"\nTotal number of samples processed from manifest(s): "
                f"{len(manifest.keys())}"
            )

            not_processed = sorted(
                set(summary.get('provided_manifest_samples')) -
                set(manifest.keys())
            )
            file_handle.write(
                f"\nSamples from manifest(s) not processed "
                f"({len(not_processed)}): "
                f"{', '.join(not_processed) if not_processed else 'None'}\n"
            )

        if summary.get('excluded'):
            file_handle.write(
                "\nSamples specified to exclude from CNV calling and CNV "
                f"reports ({len(summary.get('excluded'))}): "
                f"{', '.join(sorted(summary.get('excluded')))}"
            )

        if summary.get('cnv_call_excluded'):
            file_handle.write(
                "\nFiles matched and excluded from CNV calling "
                f"({len(summary.get('cnv_call_excluded'))}): "
                f"{', '.join(sorted(summary.get('cnv_call_excluded')))}"
            )

        launched_jobs = '\n\t'.join([
            f"{k} : {len(v)} jobs" if len(v) > 1
            else f"{k} : {len(v)} job"
            for k, v in summary.get('launched_jobs').items()
        ])

        file_handle.write(f"\nTotal jobs launched:\n\t{launched_jobs}\n")

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
        if summary.get('mosaic_report_summary'):
            outputs = {**outputs, **summary.get('mosaic_report_summary')}

        if outputs:
            fancy_table = pd.DataFrame(outputs)
            fancy_table.fillna(value='-', inplace=True)
            fancy_table = fancy_table.to_markdown(tablefmt="grid")
            file_handle.write(
                f"\nReports created per sample:\n\n{fancy_table}"
            )

    # dump written file into logs
    print('\n'.join(open(output, 'r').read().splitlines()))
