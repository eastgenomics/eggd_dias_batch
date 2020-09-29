#!/usr/bin/python

import subprocess

from config import (
    mqc_applet_id,
    mqc_config_file
)
from general_functions import (
    get_dx_cwd_project_id,
    get_object_attribute_from_object_id_or_path,
    dx_make_workflow_dir
)

# MultiQC


def run_multiqc_app(ms_workflow_out_dir):
    assert ms_workflow_out_dir.startswith("/"), (
        "Input directory must be full path (starting at /)")

    project_id = get_dx_cwd_project_id()
    path_dirs = [x for x in ms_workflow_out_dir.split("/") if x]
    assert path_dirs[-3] == "output"
    assert "single" in path_dirs[-2]
    assert "multi" in path_dirs[-1]
    ss_for_multiqc = path_dirs[-2]
    ms_for_multiqc = path_dirs[-1]

    mqc_applet_name = get_object_attribute_from_object_id_or_path(
        mqc_applet_id, "Name"
    )
    mqc_applet_out_dir = "".join([ms_workflow_out_dir, mqc_applet_name])

    dx_make_workflow_dir(mqc_applet_out_dir)

    command = (
        "dx run {} --yes -ieggd_multiqc_config_file='{}' "
        "-iproject_for_multiqc='{}' -iss_for_multiqc='{}' "
        "-ims_for_multiqc='{}' --destination='{}'"
    ).format(
        mqc_applet_id, mqc_config_file,
        project_id, ss_for_multiqc,
        ms_for_multiqc, mqc_applet_out_dir
    )

    subprocess.check_call(command, shell=True)

    return mqc_applet_out_dir
