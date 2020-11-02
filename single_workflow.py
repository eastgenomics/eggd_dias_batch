#!/usr/bin/python

import os
import subprocess
import uuid

from config import (
    ss_workflow_id,
    sentieon_R1_input_stage,
    sentieon_R2_input_stage,
    sentieon_sample_input_stage,
    fastqc_fastqs_input_stage,
)
from general_functions import (
    get_object_attribute_from_object_id_or_path,
    get_date,
    dx_make_workflow_dir,
    format_relative_paths,
    get_workflow_stage_info,
    make_app_out_dirs
)

# Single sample apps batch


def make_ss_workflow_out_dir(workflow_id):
    workflow_name = get_object_attribute_from_object_id_or_path(
        workflow_id, "Name"
    )
    assert workflow_name, "Workflow name not found. Aborting"
    workflow_output_dir_pattern = "/output/{workflow_name}-{date}-{index}/"
    date = get_date()

    i = 1
    while i < 100:  # < 100 runs = sanity check
        workflow_output_dir = workflow_output_dir_pattern.format(
            workflow_name=workflow_name, date=date, index=i
        )

        if dx_make_workflow_dir(workflow_output_dir):
            print("Using\t\t%s" % workflow_output_dir)
            return workflow_output_dir
        else:
            print("Skipping\t%s" % workflow_output_dir)

        i += 1

    return None


def make_ss_dias_batch_file(input_directory):
    # uuids for temp files to prevent collisions during parallel runs
    fastq_dict = make_fq_dict(input_directory)
    batch_uuid = str(uuid.uuid4())
    batch_tsv = batch_uuid + ".dx_batch.tsv"

    id_suffix = " ID"

    headers = [
        "batch ID",
        sentieon_R1_input_stage,
        sentieon_R2_input_stage,
        sentieon_sample_input_stage,
        fastqc_fastqs_input_stage,
        sentieon_R1_input_stage + id_suffix,
        sentieon_R2_input_stage + id_suffix,
        fastqc_fastqs_input_stage + id_suffix
    ]

    batch_file_lines = []
    header_line = "\t".join(headers)
    batch_file_lines.append(header_line)

    for sample, reads in sorted(fastq_dict.items()):
        assert(len(reads["R1"]) == len(reads["R2"])), \
            "Mismatched number of R1/R2 fastqs for {}".format(sample)
        r_1 = "[" + ",".join(reads["R1"]) + "]"
        r_2 = "[" + ",".join(reads["R2"]) + "]"
        r_all = "[" + ",".join(
            [",".join(reads["R1"]), ",".join(reads["R2"])]
        ) + "]"
        data_fields = [sample, "-", "-", sample, "-", r_1, r_2, r_all]
        data_line = "\t".join(data_fields)
        batch_file_lines.append(data_line)

    with open(batch_tsv, "w") as batch_fh:
        for line in batch_file_lines:
            batch_fh.write("%s\n" % line)

    assert os.path.exists(batch_tsv), "Failed to generate batch file!"
    return batch_tsv


def make_fq_dict(path):

    command = "dx find data --path {} --name *fastq.gz --brief".format(path)

    fastq_id_list = subprocess.check_output(
        command, shell=True
    ).strip().split("\n")

    fastq_dict = {}

    for fastq_id in fastq_id_list:
        command = "dx describe --name {}".format(fastq_id)
        fastq_file_id = fastq_id.split(":")[1]
        fastq_filename = subprocess.check_output(command, shell=True).strip()
        sample_id = fastq_filename.split("_")[0]

        if sample_id == "Undetermined":
            continue

        read_num = None
        if "_R1_" in fastq_filename:
            read_num = "R1"
        elif "_R2_" in fastq_filename:
            read_num = "R2"

        assert read_num, (
            "Unable to determine read number (R1 or R2) for fastq {}".format(
                fastq_filename
            )
        )

        # Make a new dict entry for sample if not present
        fastq_dict.setdefault(sample_id, {
            "R1": [],
            "R2": []}
        )

        # Add fastq filename and file_id to appropriate place in dict.
        # We add both id and name because we need to sort by name later
        fastq_dict[sample_id].setdefault(read_num, []).append(
            (fastq_filename, fastq_file_id)
        )

    # Sort fastq lists so that the fastq at pos n in R1 list
    # is paired with the fastq at pos n in R2 list
    # Once the sort is complete we remove the filename from the dict
    # since it was only there to enable the sort
    for sample in fastq_dict:
        for read in ["R1", "R2"]:
            # sort tuple on first element i.e. filename
            # retain only file id i.e. second element
            sorted_fastq_list = [
                x[1]
                for x in sorted(fastq_dict[sample][read], key=lambda x: x[0])
            ]
            fastq_dict[sample][read] = sorted_fastq_list

    return fastq_dict


def run_dias_ss_batch_file(
    workflow_id, batch_file, workflow_stage_info, workflow_out_dir
):
    app_relative_paths = format_relative_paths(workflow_stage_info)
    command = (
        'dx run --yes --ignore-reuse {} --batch-tsv {} --destination={} {}'
    ).format(
            workflow_id,
            batch_file,
            workflow_out_dir,
            app_relative_paths
        )
    subprocess.call(command, shell=True)


def run_ss_workflow(input_dir):
    assert input_dir.startswith("/"), (
        "Input directory must be full path (starting at /)")
    ss_workflow_out_dir = make_ss_workflow_out_dir(ss_workflow_id)
    ss_workflow_stage_info = get_workflow_stage_info(ss_workflow_id)
    ss_app_out_dirs = make_app_out_dirs(
        ss_workflow_stage_info, ss_workflow_out_dir
    )
    ss_batch_file = make_ss_dias_batch_file(input_dir)
    run_dias_ss_batch_file(
        ss_workflow_id, ss_batch_file,
        ss_workflow_stage_info, ss_workflow_out_dir)

    return ss_workflow_out_dir
