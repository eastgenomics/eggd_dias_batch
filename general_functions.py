#!/usr/bin/python

import datetime
import subprocess
import uuid


# Generic functions


def get_date():
    date = datetime.datetime.now()
    return date.strftime("%y%m%d")


def dx_make_workflow_dir(dx_dir_path):
    command = "dx mkdir -p /output/; dx mkdir {dx_dir_path}".format(
        dx_dir_path=dx_dir_path)
    try:
        subprocess.check_output(command,
                                stderr=subprocess.STDOUT,
                                shell=True)
        return True
    except subprocess.CalledProcessError as cmdexc:
        return False


def describe_object(object_id_or_path):
    command = "dx describe {object_id_or_path}".format(
        object_id_or_path=object_id_or_path)

    # This try except is used to handle permission errors generated
    # when dx describe tries to get info about files we do not have permission
    # to access.
    # In these cases the description is returned but the commands has non-0
    # exit status so errors out

    try:
        workflow_description = subprocess\
            .check_output(command, stderr=subprocess.STDOUT, shell=True)
    except subprocess.CalledProcessError as cmdexc:
        workflow_description = str(cmdexc.output)
    return workflow_description.split("\n")


def get_object_attribute_from_object_id_or_path(object_id_or_path, attribute):

    workflow_description = describe_object(object_id_or_path)

    for line in workflow_description:
        if line.startswith("{attribute} ".format(attribute=attribute)):
            attribute_value = line.split(" ")[-1]
            return attribute_value
    return None


def get_workflow_stage_info(workflow_id):
    workflow_description = describe_object(workflow_id)

    stages = {}

    previous_line_is_stage = False

    for index, line in enumerate(workflow_description):

        if line.startswith("Stage "):
            previous_line_is_stage = True
            stage = line.split(" ")[1]

        # If prev line was stage line then this line contains executable
        elif previous_line_is_stage:
            error_msg = "Expected \'Executable\' line after stage line \
                {line_num}\n{line}".format(line_num=index+1, line=line)

            assert line.startswith("  Executable"), error_msg

            app_id = line.split(" ")[-1]
            app_name = get_object_attribute_from_object_id_or_path(
                app_id, "Name"
            )

            stages[stage] = {
                "app_id": app_id, "app_name": app_name
            }
            previous_line_is_stage = False

        else:
            previous_line_is_stage = False

    return stages


def make_app_out_dirs(workflow_stage_info, workflow_output_dir):
    out_dirs = {}

    for stage, stage_info in sorted(workflow_stage_info.items()):
        app_out_dir = "{workflow_output_dir}{app_name}".format(
            workflow_output_dir=workflow_output_dir,
            app_name=stage_info["app_name"]
        )
        # mkdir with -p so no error if multiples of same app try to make
        # multiple dirs e.g. fastqc
        command = "dx mkdir -p {app_out_dir}"\
            .format(app_out_dir=app_out_dir)

        subprocess.check_output(command, shell=True)
        out_dirs[stage] = app_out_dir
    return out_dirs


def format_relative_paths(workflow_stage_info):
    result = ""
    for stage_id, stage_info in sorted(workflow_stage_info.items()):
        command_option = '--stage-relative-output-folder {} "{}" '.format(
            stage_id, stage_info["app_name"]
        )
        result += command_option
    return result


def find_app_dir(workflow_output_dir, app_basename):
    if app_basename is None:
        return "/"

    command = "dx ls {workflow_output_dir} | grep {app_basename}".format(
        workflow_output_dir=workflow_output_dir, app_basename=app_basename
    )

    search_result = subprocess.check_output(
        command, shell=True
    ).strip().split("\n")

    assert len(search_result) < 2, (
        "app_basename '{}' resolved to multiple apps:\n{apps}".format(
            app_basename, apps=", ".join(search_result)
        )
    )
    assert len(search_result) > 0, "app_basename '{}' matched no apps".format(
        app_basename
    )
    return "".join([workflow_output_dir, search_result[0]])


def get_stage_input_file_list(app_dir, app_subdir="", filename_pattern="."):
    command = "dx ls {app_dir}{app_subdir} | grep {filename_pattern}".format(
        app_dir=app_dir, app_subdir=app_subdir,
        filename_pattern=filename_pattern
    )

    try:
        search_result = subprocess.check_output(
            command, shell=True
        ).strip().split("\n")
    except subprocess.CalledProcessError:  # If no files found grep returns 1. This can be valid e.g. no NA12878
        search_result = []

    file_ids = []

    for file in search_result:
        full_path = "".join([app_dir, app_subdir, file])
        file_id = get_object_attribute_from_object_id_or_path(full_path, "ID")
        file_ids.append(file_id)

    return file_ids


def get_dx_cwd_project_id():
    command = (
        'dx env | grep -P "Current workspace\t" | '
        'awk -F "\t" \'{print $NF}\' | sed s/\'"\'//g'
    )
    project_id = subprocess.check_output(command, shell=True).strip()
    return project_id


def parse_sample_sheet(sample_sheet_path):
    sample_ids = []
    cmd = "dx cat {}".format(sample_sheet_path).split()
    sample_sheet_content = subprocess.check_output(cmd).split("\n")

    data = False
    index = 0

    for line in sample_sheet_content:
        if line:
            if data is True:
                line = line.split(",")

                if index == 0:
                    # get column of sample_id programmatically
                    sample_id_pos = [
                        i
                        for i, header in enumerate(line)
                        if header == "Sample_ID"
                    ][0]
                else:
                    sample_ids.append(line[sample_id_pos])

                index += 1
            else:
                if line.startswith("[Data]"):
                    data = True

    return sample_ids


def fill_stage_input_dict(workflow_out_dir, stage_input_dict):
    for stage_input, stage_input_info in stage_input_dict.items():
        # workflow_out_dir = "/output/dias_v1.0.0_DEV-200430-1/"  # DEBUG
        input_app_dir = find_app_dir(
            workflow_out_dir, stage_input_info["app"]
        )
        stage_input_dict[stage_input]["file_list"] = get_stage_input_file_list(
            input_app_dir, app_subdir=stage_input_info["subdir"],
            filename_pattern=stage_input_info["pattern"]
        )

    return stage_input_dict


def make_dias_batch_file(
    stage_input_dict, workflow_type, workflow_specificity
):
    batch_uuid = str(uuid.uuid4())
    batch_filename = ".".join([batch_uuid, "tsv"])

    headers = ["batch ID"]
    values = []

    if workflow_type == "multi":
        values.append("multi")

        # Hap.py - static values
        headers.append(workflow_specificity)
        values.append("NA12878")
    elif workflow_type == "reports":
        values.append("reports")

        for stage, file_id in workflow_specificity.iteritems():
            # dynamic files to be added in the batch_tsv
            headers.append(stage)
            values.append("")
            values.append(file_id)

    # For each stage add the column header and the values in that column
    for stage_input in stage_input_dict:

        if len(stage_input_dict[stage_input]["file_list"]) == 0:
            continue

        headers.append(stage_input)  # col for file name
        headers.append(" ".join([stage_input, "ID"]))  # col for file ID

        # One file in file list - no need to merge into array
        if len(stage_input_dict[stage_input]["file_list"]) == 1:
            file_ids = stage_input_dict[stage_input]["file_list"][0]
            values.append("")  # No need to provide file name in batch file
            values.append(file_ids)

        # make a square bracketed comma separated list if multiple input files
        elif len(stage_input_dict[stage_input]["file_list"]) > 1:
            # Square bracketed csv list
            file_id_list = [
                file_id
                for file_id in stage_input_dict[stage_input]["file_list"]
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


def make_workflow_out_dir(workflow_id, workflow_out_dir="/output/"):
    workflow_name = get_object_attribute_from_object_id_or_path(
        workflow_id, "Name"
    )
    assert workflow_name, "Workflow name not found. Aborting"

    workflow_dir = "{}{}".format(workflow_out_dir, workflow_name)

    workflow_output_dir_pattern = "{workflow_dir}-{date}-{index}/"
    date = get_date()

    i = 1
    while i < 100:  # < 100 runs = sanity check
        workflow_output_dir = workflow_output_dir_pattern.format(
            workflow_dir=workflow_dir, date=date, index=i
        )

        if dx_make_workflow_dir(workflow_output_dir):
            print("Using\t\t%s" % workflow_output_dir)
            return workflow_output_dir
        else:
            print("Skipping\t%s" % workflow_output_dir)

        i += 1

    return None
