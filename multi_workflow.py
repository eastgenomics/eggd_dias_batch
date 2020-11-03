#!/usr/bin/python

from collections import OrderedDict
import json
import subprocess

from config import (
    multi_stage_input_dict, ms_workflow_id, happy_stage_prefix
)
from general_functions import (
    format_relative_paths,
    get_workflow_stage_info,
    make_app_out_dirs,
    fill_stage_input_dict,
    make_dias_batch_file,
    make_workflow_out_dir
)

# Multi sample apps


def run_ms_workflow(ss_workflow_out_dir, dry_run):
    assert ss_workflow_out_dir.startswith("/"), (
        "Input directory must be full path (starting at /)")
    ms_workflow_out_dir = make_workflow_out_dir(
        ms_workflow_id, ss_workflow_out_dir
    )
    ms_workflow_stage_info = get_workflow_stage_info(ms_workflow_id)
    ms_output_dirs = make_app_out_dirs(
        ms_workflow_stage_info, ms_workflow_out_dir
    )

    ms_stage_input_dict = fill_stage_input_dict(
        ss_workflow_out_dir, multi_stage_input_dict
    )
    ms_batch_file = make_dias_batch_file(
        ms_stage_input_dict, "multi", happy_stage_prefix
    )

    run_wf_command = "dx run --yes --ignore-reuse --ignore-reuse-stage * {} --batch-tsv={}".format(
        ms_workflow_id, ms_batch_file
    )

    app_relative_paths = format_relative_paths(ms_workflow_stage_info)

    destination = " --destination={} ".format(ms_workflow_out_dir)

    command = " ".join([run_wf_command, app_relative_paths, destination])

    if dry_run:
        print("Created workflow dir: {}".format(ms_workflow_out_dir))
        print("Stage info:")
        print(json.dumps(OrderedDict(
            sorted(ms_workflow_stage_info.iteritems())), indent=2)
        )
        print("Inputs gathered:")
        print(json.dumps(ms_stage_input_dict, indent=4))
        print("Created apps out dir: {}")
        print(json.dumps(OrderedDict(
            sorted(ms_output_dirs.iteritems())), indent=4)
        )
        print("Created batch tsv: {}".format(ms_batch_file))
        print("Format of stage output dirs:")
        print(app_relative_paths)
        print("Final cmd ran:")
        print(command)
    else:
        subprocess.call(command, shell=True)

    return ms_workflow_out_dir
