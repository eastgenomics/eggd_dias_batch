#!/usr/bin/python

from collections import OrderedDict
import json
import subprocess

from config import (
    rpt_stage_input_dict, rpt_dynamic_files, rpt_workflow_id,
    rea_stage_input_dict, rea_dynamic_files
)
from general_functions import (
    format_relative_paths,
    get_workflow_stage_info,
    make_app_out_dirs,
    parse_sample_sheet,
    make_workflow_out_dir,
    get_stage_inputs,
    prepare_batch_writing,
    create_batch_file,
    assess_batch_file
)

# reanalysis


def run_reanalysis(input_dir, dry_run, reanalysis_list):
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

    run_reports(input_dir, dry_run, reanalysis_dict=reanalysis_dict)


def run_reports(
    ss_workflow_out_dir, dry_run, sample_sheet_path=None, reanalysis_dict=None
):
    assert ss_workflow_out_dir.startswith("/"), (
        "Input directory must be full path (starting at /)")
    rpt_workflow_out_dir = make_workflow_out_dir(
        rpt_workflow_id, ss_workflow_out_dir
    )

    rpt_workflow_stage_info = get_workflow_stage_info(rpt_workflow_id)
    rpt_output_dirs = make_app_out_dirs(
        rpt_workflow_stage_info, rpt_workflow_out_dir
    )

    sample2stage_input_dict = {}

    if reanalysis_dict:
        sample_id_list = reanalysis_dict

        for sample in sample_id_list:
            sample2stage_input_dict[sample] = rea_stage_input_dict

    else:
        sample_id_list = parse_sample_sheet(sample_sheet_path)

        for sample in sample_id_list:
            sample2stage_input_dict[sample] = rpt_stage_input_dict

    staging_dict = get_stage_inputs(
        ss_workflow_out_dir, sample2stage_input_dict
    )

    if reanalysis_dict:
        headers = []
        values = []

        rea_headers, rea_values = prepare_batch_writing(
            staging_dict, "reports", rea_dynamic_files
        )

        for header in rea_headers:
            new_headers = [field for field in header]
            new_headers.append(
                "stage-Fyq5ypj433GzxPK360B8Qfg5.list_panel_names_genes"
            )
            new_headers.append("stage-Fyq5yy0433GXxz691bKyvjPJ.panel")
            headers.append(tuple(new_headers))

        for line in rea_values:
            panels = [
                list(panel) for sample, panel in reanalysis_dict.items()
                if line[0] == sample
            ][0]
            line.extend(panels)
            line.extend(panels)

            values.append(line)
    else:
        headers, values = prepare_batch_writing(
            staging_dict, "reports", rpt_dynamic_files
        )

    rpt_batch_file = create_batch_file(headers, values)

    command = "dx run -y --rerun-stage '*' {} --batch-tsv={}".format(
        rpt_workflow_id, rpt_batch_file
    )
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
        print(json.dumps(staging_dict, indent=4))
        print("Created apps out dir: {}")
        print(json.dumps(
            OrderedDict(sorted(rpt_output_dirs.iteritems())), indent=4)
        )
        print("Created batch tsv: {}".format(rpt_batch_file))

        check_batch_file = assess_batch_file(rpt_batch_file)

        if check_batch_file is True:
            print(
                "{}: Format of the file is correct".format(rpt_batch_file)
            )
        else:
            print((
                "Number of columns in header doesn't match "
                "nb of columns in values at line {}".format(check_batch_file)
            ))

        print("Format of stage output dirs: {}".format(app_relative_paths))
        print("Final cmd ran: {}".format(command))
        print("Deleting '{}' as part of the dry-run".format(rpt_workflow_out_dir))
        delete_folders_cmd = "dx rm -r {}".format(rpt_workflow_out_dir)
        subprocess.call(delete_folders_cmd, shell=True)
    else:
        subprocess.call(command, shell=True)

    return rpt_workflow_out_dir
