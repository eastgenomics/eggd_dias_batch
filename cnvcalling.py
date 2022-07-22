#!/usr/bin/python
import sys
import subprocess

from general_functions import (
    get_dx_cwd_project_name,
    get_object_attribute_from_object_id_or_path,
    dx_make_workflow_dir,
    find_app_dir,
    get_stage_input_file_list
)


def find_files(app_dir, pattern="."):
    command = "dx ls {app_dir} | grep {pattern}".format(
        app_dir=app_dir, pattern=pattern
    )

    try:
        search_result = subprocess.check_output(
            command, shell=True
        ).strip().split("\n")
    except subprocess.CalledProcessError:
        search_result = []

    return search_result


# Run-level CNV calling of samples that passed QC


def run_cnvcall_app(ss_workflow_out_dir, dry_run, assay_config, assay_id, sample_list):
    # Find project to create jobs and outdirs in
    project_name = get_dx_cwd_project_name()

    # Make sure path provided is an actual ss workflow output folder
    assert ss_workflow_out_dir.startswith("/"), (
        "Input directory must be full path (starting at /)")
    path_dirs = [x for x in ss_workflow_out_dir.split("/") if x]
    ss_for_multiqc = [ele for ele in path_dirs if "single" in ele]
    assert ss_for_multiqc != [], (
        "Path '{}' is not an accepted directory, "
        "must contain 'single'".format(ss_workflow_out_dir)
    )
    ss_for_multiqc = ss_for_multiqc[0]

    # Find the app name and create an output folder for it under ss
    app_name = get_object_attribute_from_object_id_or_path(
        assay_config.cnvcall_app_id, "Name"
    )
    app_out_dir = "".join([ss_workflow_out_dir, app_name])
    dx_make_workflow_dir(app_out_dir)

    # Find bam and bai files from sentieon folder
    bambi_files = []
    folder_path = find_app_dir(ss_workflow_out_dir, assay_config.cnvcalling_input_dict["app"])
    extensions = assay_config.cnvcalling_input_dict["patterns"]
    for ext in extensions:
        bambi_files.extend(find_files(folder_path, ext))

    # Read in list of samples that did NOT PASS QC
    sample_names = []
    # parse sample exclusion file
    with open(sample_list) as fh:
        for line in fh:  # line can be a sample name or sample tab panel name
            sample_names.append(line.strip().split("\t")[0])

    # Remove bam/bai files of QC faild samples
    sample_bambis = [x for x in bambi_files if x.split('_')[0] not in sample_names]

    # Find the file-IDs of the passed bam/bai samples
    file_ids = ""

    for file in sample_bambis:
        full_path = "".join([folder_path, file])
        file_id = get_object_attribute_from_object_id_or_path(full_path, "ID")
        file_ids += " -ibambais=" + file_id

    command = (
        "dx run {} --yes --ignore-reuse -iGATK_docker='{}' "
        "-iinterval_list='{}' -iannotation_tsv='{}' {} "
        "-idebug_fail_start=False -idebug_fail_end=False "
        "-irun_name='{}' "
        "--destination='{}'"
    ).format(
        assay_config.cnvcall_app_id,
        assay_config.cnvcalling_fixed_inputs["gatk_docker"],
        assay_config.cnvcalling_fixed_inputs["interval_list"],
        assay_config.cnvcalling_fixed_inputs["annotation_tsv"],
        file_ids,
        project_name, app_out_dir
    )

    if dry_run is True:
        print("Final cmd ran: {}".format(command))
    else:
        subprocess.call(command, shell=True)

    return app_out_dir
