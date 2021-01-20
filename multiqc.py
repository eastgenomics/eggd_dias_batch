#!/usr/bin/python

import subprocess

from config import (
    mqc_applet_id,
    mqc_config_file
)

from general_functions import (
    get_dx_cwd_project_id, get_object_attribute_from_object_id_or_path,
    dx_make_workflow_dir
)

# MultiQC


def run_multiqc_app(ms_workflow_out_dir, dry_run):
    assert ms_workflow_out_dir.startswith("/"), (
        "Input directory must be full path (starting at /)")

    project_id = get_dx_cwd_project_id()
    path_dirs = [x for x in ms_workflow_out_dir.split("/") if x]
    ss_for_multiqc = [ele for ele in path_dirs if "single" in ele]
    ms_for_multiqc = [ele for ele in path_dirs if "multi" in ele]

    assert ss_for_multiqc != [], (
        "Path '{}' is not an accepted directory, "
        "must contain 'single'".format(ms_workflow_out_dir)
    )

    ss_for_multiqc = ss_for_multiqc[0]

    if ms_for_multiqc != []:
        multi_folder = "-ims_for_multiqc='{}'".format(ms_for_multiqc[0])
    else:
        multi_folder = ""

    mqc_applet_name = get_object_attribute_from_object_id_or_path(
        mqc_applet_id, "Name"
    )
    mqc_applet_out_dir = "".join([ms_workflow_out_dir, mqc_applet_name])

    dx_make_workflow_dir(mqc_applet_out_dir)

    command = (
        "dx run {} --yes --ignore-reuse -ieggd_multiqc_config_file='{}' "
        "-iproject_for_multiqc='{}' -iss_for_multiqc='{}' "
        "{} --destination='{}'"
    ).format(
        mqc_applet_id, mqc_config_file,
        project_id, ss_for_multiqc,
        multi_folder, mqc_applet_out_dir
    )

    if dry_run is True:
        print("Final cmd ran: {}".format(command))
    else:
        subprocess.call(command, shell=True)

    return mqc_applet_out_dir
