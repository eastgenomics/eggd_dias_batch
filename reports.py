#!/usr/bin/python

from collections import OrderedDict
import json
import subprocess

from config import (
    rpt_stage_input_dict, dynamic_files_batch_tsv, rpt_workflow_id
)
from general_functions import (
    format_relative_paths,
    fill_stage_input_dict,
    get_workflow_stage_info,
    make_app_out_dirs,
    make_dias_batch_file,
    make_workflow_out_dir
)

# vcf2xls


def make_reanalysis_batch_file(batch_file, reanalysis_dict):

    # We have an input batch file with all samples on the run and
    # no panels specified
    # Discard the samples we do not want, and add panels to those we do want
    # This is done in this way because a batch file cannot be generated
    # for a specific subset of sampleIDs, so instead we make a batch for all
    # and remove those we don't need

    output_lines = []

    with open(batch_file) as in_fh:

        # Add a new column for the panels
        header = in_fh.readline().strip()
        output_header = "\t".join([header, "list_panel_names_genes\n"])
        output_lines.append(output_header)

        # Find the samples we want and add their panels
        for line in in_fh:
            sample = line.split("\t")[0]
            panels = reanalysis_dict.get(sample, None)

            # If no panel(s) then not a reanalysis so dont include in output
            if panels:
                panels_str = ",".join(list(panels))
                output_line = "\t".join([line.strip(), panels_str + "\n"])
                output_lines.append(output_line)

    with open(batch_file, "w") as out_fh:
        out_fh.writelines(output_lines)

    return batch_file


# coverage report

def reports():
    pass


# reanalysis

def run_reanalysis(input_dir, reanalysis_list):
    reanalysis_dict = {}

    with open(reanalysis_list) as r_fh:
        for line in r_fh:
            fields = line.strip().split("\t")
            assert len(fields) == 2, (
                "Unexpected number of fields in reanalysis_list. "
                "File must contain one tab separated "
                "sample/panel combination per line"
            )
            sample, panel = fields
            reanalysis_dict.setdefault(sample, set()).add(panel)

    run_reports(input_dir, reanalysis_dict)


def run_reports(ss_workflow_out_dir, dry_run, reanalysis_dict=None):
    assert ss_workflow_out_dir.startswith("/"), (
        "Input directory must be full path (starting at /)")
    rpt_workflow_out_dir = make_workflow_out_dir(
        rpt_workflow_id, ss_workflow_out_dir
    )

    rpt_workflow_stage_info = get_workflow_stage_info(rpt_workflow_id)
    rpt_output_dirs = make_app_out_dirs(
        rpt_workflow_stage_info, rpt_workflow_out_dir
    )

    rpt_staging_dict = fill_stage_input_dict(
        ss_workflow_out_dir, rpt_stage_input_dict
    )
    rpt_batch_file = make_dias_batch_file(
        rpt_staging_dict, "reports", dynamic_files_batch_tsv
    )

    if reanalysis_dict:
        batch_file = make_reanalysis_batch_file(rpt_batch_file, reanalysis_dict)

    command = "dx run {}".format()
    app_relative_paths = format_relative_paths(rpt_workflow_stage_info)
    destination = " --destination={} ".format(rpt_workflow_out_dir)

    command = " ".join([command, app_relative_paths, destination])

    if dry_run:
        print("Created workflow dir: {}".format(rpt_workflow_out_dir))
        print("Stage info:")
        print(json.dumps(
            OrderedDict(sorted(rpt_workflow_stage_info.iteritems())), indent=2)
        )
        print("Inputs gathered:")
        print(json.dumps(rpt_staging_dict, indent=4))
        print("Created apps out dir: {}")
        print(json.dumps(
            OrderedDict(sorted(rpt_output_dirs.iteritems())), indent=4)
        )
        print("Created batch tsv: {}".format(rpt_batch_file))
        print("Format of stage output dirs: {}".format(app_relative_paths))
        print("Final cmd ran: {}".format(command))
    else:
        subprocess.call(command, shell=True)

    return rpt_workflow_out_dir
