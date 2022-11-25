#!/usr/bin/python
from ast import excepthandler
import sys
import subprocess
import dxpy

from general_functions import (
    get_dx_cwd_project_name,
    get_object_attribute_from_object_id_or_path,
    find_app_dir,
    make_app_output_dir,
    find_files
)

# Run-level CNV calling of samples that passed QC


def run_cnvcall_app(ss_workflow_out_dir, dry_run, assay_config, assay_id, excluded_sample_list):
    """Sets off the CNV calling app.

    Args:
        ss_workflow_out_dir (str): path to single output
        dry_run (str): optional boolean whether this is a dry/test run
        assay_config (variable): assay config file containing all variables
        excluded_sample_list (file): optional file containg list of samples to exclude

    Returns:
        app_out_dir: path to where the CNV calling output will be
    """
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


    app_output_dir = make_app_output_dir(assay_config.cnvcall_app_id, ss_workflow_out_dir, app_name, assay_id)

    # Find bam and bai files from sentieon folder
    bambi_files = []
    folder_path = find_app_dir(ss_workflow_out_dir, assay_config.cnvcalling_input_dict["app"])
    extensions = assay_config.cnvcalling_input_dict["patterns"]
    for ext in extensions:
        bambi_files.extend(find_files(project_name, folder_path, ext))

    # Read in list of samples that did NOT PASS QC
    if excluded_sample_list is None:
        sample_names = []
    else:
        sample_names = []
        # parse sample exclusion file
        with open(excluded_sample_list) as fh:
            for line in fh:  # line can be a sample name or sample tab panel name
                sample_names.append(line.strip().split("\t")[0])

    # Get the first part of sample_names
    sample_names = [x.split('_')[0] for x in sample_names]
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
        project_name, app_output_dir
    )
    # upload excluded regions file
    excluded_list_path = app_output_dir + "/" + "excluded_list.tsv"
    cmd = "dx upload {} --path {}".format(excluded_sample_list, excluded_list_path)
    subprocess.check_output(cmd, shell=True)

    if dry_run is True:
        print("Final cmd ran: {}".format(command))
    else:
        subprocess.call(command, shell=True)

    return app_output_dir
