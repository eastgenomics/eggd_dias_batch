import subprocess

from general_functions import (
    get_dx_cwd_project_id, describe_object, get_sample_ids_from_sample_sheet
)


def check_if_all_reports_created(vcf2xls_dir, sample_sheet):
    """ Return the difference between the samples in the sample sheet
    the sample ids ending with _1.xls gathered from the vcf2xls directory

    Args:
        vcf2xls_dir (str): Directory in DNAnexus containing the initial batch of reports
        sample_sheet (str): DNAnexus path to the samplesheet

    Returns:
        set: Set of samples ids missing from the report list compared to the sample sheet
    """

    report_list = []
    sample_id_list = get_sample_ids_from_sample_sheet(sample_sheet)
    field, project_name = describe_object(get_dx_cwd_project_id())[3].split()
    assert field == "Name", (
        "DNAnexus describe() may have changed positions of fields. "
        "Debugging is needed"
    )
    print(project_name)
    cmd = "dx ls {}".format(vcf2xls_dir)
    list_of_created_reports = subprocess.check_output(
        cmd, shell=True
    ).split("\n")

    # get reports generated in first iteration
    for report in list_of_created_reports:
        if report.endswith("_1.xls"):
            sample_id = report[:-6]
            report_list.append(sample_id)

    difference = set(sample_id_list).difference(set(report_list))

    return difference
