import subprocess

from general_functions import (
    get_dx_cwd_project_id, describe_object, get_sample_ids_from_sample_sheet
)


def check_if_all_reports_created(vcf2xls_dir, sample_sheet):
    report_list = []
    sample_id_list = get_sample_ids_from_sample_sheet(sample_sheet)
    field, project_name = describe_object(get_dx_cwd_project_id())[3].split()
    assert field == "Name", (
        "DNAnexus describe() may have changed positions of fields. "
        "Debugging is needed"
    )
    print(project_name)
    cmd = "dx ls {}".format(vcf2xls_dir).split()
    list_of_created_reports = subprocess.check_output(cmd).split("\n")

    # get reports generated in first iteration
    for report in list_of_created_reports:
        if report.endswith("_1.xls"):
            # when i tried _1.xls, it would strip some of the numbers too
            # no idea why, can't be bothered to understand
            # am lazy
            report_list.append(report.strip("1.xls").strip("_"))

    difference = set(sample_id_list).difference(set(report_list))

    return difference