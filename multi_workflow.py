#!/usr/bin/python

from collections import OrderedDict
import json
import subprocess

from general_functions import (
    assess_batch_file, create_batch_file, format_relative_paths,
    get_workflow_stage_info,
    make_app_out_dirs,
    get_stage_inputs,
    prepare_batch_writing,
    make_workflow_out_dir,
    assess_batch_file
)

# Multi sample apps


def run_ms_workflow(ss_workflow_out_dir, dry_run, assay_config):
    assert ss_workflow_out_dir.startswith("/"), (
        "Input directory must be full path (starting at /)")
    ms_workflow_out_dir = make_workflow_out_dir(
        assay_config.ms_workflow_id, ss_workflow_out_dir
    )
    ms_workflow_stage_info = get_workflow_stage_info(
        assay_config.ms_workflow_id
    )
    ms_output_dirs = make_app_out_dirs(
        ms_workflow_stage_info, ms_workflow_out_dir
    )

    # create sub dict to match the changes to get_stage_inputs
    ms_input_dict = {"multi": assay_config.multi_stage_input_dict}

    # gather files for given app/pattern
    ms_stage_input_dict = get_stage_inputs(
        ss_workflow_out_dir, ms_input_dict
    )
    # get the header and values to write in the batch tsv
    ms_headers, ms_values = prepare_batch_writing(
        ms_stage_input_dict, "multi", assay_config,
        assay_config.happy_stage_bed
    )
    ms_batch_file = create_batch_file(ms_headers, ms_values)

    run_wf_command = "dx run --yes --rerun-stage '*' {} --batch-tsv={}".format(
        assay_config.ms_workflow_id, ms_batch_file
    )

    app_relative_paths = format_relative_paths(ms_workflow_stage_info)

    destination = " --destination={} ".format(ms_workflow_out_dir)

    command = " ".join([run_wf_command, app_relative_paths, destination])

    if dry_run is True:
        print("Created workflow dir: {}".format(ss_workflow_out_dir))
        print("Stage info:")
        print(json.dumps(OrderedDict(
            sorted(ms_workflow_stage_info.iteritems())), indent=2)
        )
        print("Created apps out dir: {}")
        print(json.dumps(OrderedDict(
            sorted(ms_output_dirs.iteritems())), indent=4)
        )
        print("Inputs gathered:")
        print(json.dumps(ms_stage_input_dict, indent=4))
        print("Created batch tsv: {}".format(ms_batch_file))
        print("Format of stage output dirs: {}".format(app_relative_paths))
        print("Final cmd ran: {}".format(command))

        check_batch_file = assess_batch_file(ms_batch_file)

        if check_batch_file is True:
            print(
                "{}: Format of the file is correct".format(ms_batch_file)
            )
        else:
            print((
                "Number of columns in header doesn't match "
                "nb of columns in values at line {}".format(check_batch_file)
            ))

        print("Deleting '{}' as part of the dry-run".format(ms_workflow_out_dir))
        delete_folders_cmd = "dx rm -r {}".format(ms_workflow_out_dir)
        subprocess.call(delete_folders_cmd, shell=True)
    else:
        subprocess.call(command, shell=True)

    return ms_workflow_out_dir
