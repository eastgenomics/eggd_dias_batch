#!/usr/bin/python

import subprocess
import uuid

from config import (
    stage_input_dict, ms_workflow_id
)
from general_functions import (
    get_object_attribute_from_object_id_or_path,
    get_date,
    dx_make_workflow_dir,
    format_relative_paths,
    get_workflow_stage_info,
    make_app_out_dirs,
    find_app_dir,
    get_stage_input_file_list
)

# Multi sample apps


def make_ms_workflow_out_dir(ms_workflow_id, ss_workflow_out_dir):
    ms_workflow_name = get_object_attribute_from_object_id_or_path(
        ms_workflow_id, "Name"
    )
    assert ms_workflow_name, "Workflow name not found. Aborting"
    date = get_date()

    i = 1

    while i < 100:  # < 100 runs = sanity check
        ms_workflow_output_dir = "{}{}-{}-{}/".format(
            ss_workflow_out_dir, ms_workflow_name, date, i
        )

        if dx_make_workflow_dir(ms_workflow_output_dir):
            print("Using\t\t%s" % ms_workflow_output_dir)
            return ms_workflow_output_dir
        else:
            print("Skipping\t%s" % ms_workflow_output_dir)
        i += 1
    return None


def get_ms_stage_input_dict(ss_workflow_out_dir):
    for stage_input, stage_input_info in stage_input_dict.items():
        # ss_workflow_out_dir = "/output/dias_v1.0.0_DEV-200430-1/"  # DEBUG
        input_app_dir = find_app_dir(
            ss_workflow_out_dir, stage_input_info["app"]
        )
        stage_input_dict[stage_input]["file_list"] = get_stage_input_file_list(
            input_app_dir, app_subdir=stage_input_info["subdir"],
            filename_pattern=stage_input_info["pattern"]
        )

    return stage_input_dict


def make_ms_dias_batch_file(ms_stage_input_dict):
    batch_uuid = str(uuid.uuid4())
    batch_filename = ".".join([batch_uuid, "tsv"])

    headers = ["batch ID"]
    values = ["multi"]

    # Hap.py - static values
    headers.append("stage-Fq1BPKj433Gx3K4Y8J35j0fv.prefix")
    values.append("NA12878")

    # For each stage add the column header and the values in that column
    for stage_input in ms_stage_input_dict:

        if len(ms_stage_input_dict[stage_input]["file_list"]) == 0:
            continue

        headers.append(stage_input)  # col for file name
        headers.append(" ".join([stage_input, "ID"]))  # col for file ID

        # One file in file list - no need to merge into array
        if len(ms_stage_input_dict[stage_input]["file_list"]) == 1:
            file_ids = ms_stage_input_dict[stage_input]["file_list"][0]
            values.append("")  # No need to provide file name in batch file
            values.append(file_ids)

        # make a square bracketed comma separated list if multiple input files
        elif len(ms_stage_input_dict[stage_input]["file_list"]) > 1:
            # Square bracketed csv list
            file_id_list = [
                file_id
                for file_id in ms_stage_input_dict[stage_input]["file_list"]
            ]
            file_ids = "[{file_ids}]".format(file_ids=",".join(file_id_list))
            values.append("")  # No need to provide file name in batch file
            values.append(file_ids)

    # Write the file content
    with open(batch_filename, "w") as b_fh:
        for line in [headers, values]:
            tsv_line = "\t".join(line) + "\n"
            b_fh.write(tsv_line)

    return batch_filename


def run_ms_workflow(ss_workflow_out_dir):
    assert ss_workflow_out_dir.startswith("/"), (
        "Input directory must be full path (starting at /)")
    ms_workflow_out_dir = make_ms_workflow_out_dir(
        ms_workflow_id, ss_workflow_out_dir
    )
    ms_workflow_stage_info = get_workflow_stage_info(ms_workflow_id)
    ms_output_dirs = make_app_out_dirs(
        ms_workflow_stage_info, ms_workflow_out_dir
    )

    ms_stage_input_dict = get_ms_stage_input_dict(ss_workflow_out_dir)
    ms_batch_file = make_ms_dias_batch_file(ms_stage_input_dict)

    run_wf_command = "dx run --yes {} --batch-tsv={}".format(
        ms_workflow_id, ms_batch_file
    )

    app_relative_paths = format_relative_paths(ms_workflow_stage_info)

    destination = " --destination={} ".format(ms_workflow_out_dir)

    command = " ".join([run_wf_command, app_relative_paths, destination])
    subprocess.check_call(command, shell=True)

    return ms_workflow_out_dir
