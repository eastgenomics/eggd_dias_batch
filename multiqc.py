#!/usr/bin/python

import subprocess

from general_functions import (
    get_dx_cwd_project_name, get_object_attribute_from_object_id_or_path,
    dx_make_workflow_dir
)

# MultiQC


def run_multiqc_app(ms_workflow_out_dir, dry_run, assay_config, assay_id):
    assert ms_workflow_out_dir.startswith("/"), (
        "Input directory must be full path (starting at /)")

    project_name = get_dx_cwd_project_name()
    path_dirs = [x for x in ms_workflow_out_dir.split("/") if x]
    ss_for_multiqc = [ele for ele in path_dirs if "single" in ele]
    ms_for_multiqc = [ele for ele in path_dirs if "multi" in ele]

    assert ss_for_multiqc != [], (
        "Path '{}' is not an accepted directory, "
        "must contain 'single'".format(ms_workflow_out_dir)
    )

    ss_for_multiqc = ss_for_multiqc[0]

    if ms_for_multiqc != []:
        multi_folder = "-isecondary_workflow_output='{}'".format(ms_for_multiqc[0])
    else:
        multi_folder = ""

    mqc_applet_name = get_object_attribute_from_object_id_or_path(
        assay_config.mqc_applet_id, "Name"
    )
    mqc_applet_out_dir = "".join([ms_workflow_out_dir, mqc_applet_name])

    dx_make_workflow_dir(mqc_applet_out_dir)

    command = (
        "dx run {} --yes --ignore-reuse -imultiqc_config_file='{}' "
        "-iproject_for_multiqc='{}' -iprimary_workflow_output='{}' "
        "{} --destination='{}'"
    ).format(
        assay_config.mqc_applet_id, assay_config.mqc_config_file,
        project_name, ss_for_multiqc, multi_folder, mqc_applet_out_dir
    )

    if "TWE" in assay_config.assay_name:
        command += " --instance-type {}".format("mem1_ssd1_v2_x8")

    if dry_run is True:
        print("Final cmd ran: {}".format(command))
    else:
        subprocess.call(command, shell=True)

    return mqc_applet_out_dir
