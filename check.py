import subprocess

from general_functions import parse_sample_sheet


def check_if_all_reports_created(vcf2xls_dir, sample_sheet):
    report_list = []
    sample_id_list = parse_sample_sheet(sample_sheet)

    cmd = "dx ls {}".format(vcf2xls_dir).split()
    list_of_created_reports = subprocess.check_output(cmd).split("\n")

    # get reports generated in first iteration
    for report in list_of_created_reports:
        if report.endswith("_1.xls"):
            report_list.append(report.strip("_1.xls"))

    difference = set(sample_id_list).difference(set(report_list))

    return difference
