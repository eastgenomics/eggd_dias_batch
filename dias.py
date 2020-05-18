import datetime
import subprocess
import uuid
import os
import sys
import pprint
import argparse

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
    print("Not found")
    return None


def make_ss_workflow_out_dir(workflow_id):
    workflow_name = get_object_attribute_from_object_id_or_path(workflow_id, "Name")
    assert workflow_name, "Workflow name not found. Aborting"
    workflow_output_dir_pattern = "/output/{workflow_name}-{date}-{index}/"
    date=get_date()

    i = 1
    while i < 100:  # < 100 runs = sanity check
        workflow_output_dir = workflow_output_dir_pattern\
            .format(workflow_name=workflow_name, date=date, index=i)
        if dx_make_workflow_dir(workflow_output_dir):
            print("Using\t\t%s" % workflow_output_dir)
            return workflow_output_dir
        else:
            print("Skipping\t%s" % workflow_output_dir)
        i += 1
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
            assert line.startswith("  Executable"),\
            "Expected '  Executable' line after stage line {line_num}\n{line}"\
                .format(line_num=index+1, line=line)

            app_id = line.split(" ")[-1]
            app_name = get_object_attribute_from_object_id_or_path(app_id, "Name")

            stages[stage] = {"app_id": app_id,
                             "app_name": app_name}
            previous_line_is_stage = False

        else:
            previous_line_is_stage = False

    return stages


def make_app_out_dirs(workflow_stage_info,
                      workflow_id,
                      workflow_output_dir):
    out_dirs = {}
    for stage, stage_info in sorted(workflow_stage_info.items()):
        app_out_dir = "{workflow_output_dir}{app_name}"\
            .format(workflow_output_dir=workflow_output_dir,
                    app_name=stage_info["app_name"])
        # mkdir with -p so no error if multiples of same app try to make
        # multiple dirs e.g. fastqc
        command = "dx mkdir -p {app_out_dir}"\
            .format(app_out_dir=app_out_dir)

        subprocess.check_output(command, shell=True)
        out_dirs[stage] = app_out_dir
    return out_dirs


def make_ss_dias_batch_file(input_directory=None):
    # uuids for temp files to prevent collisions during parallel runs
    temp_uuid = str(uuid.uuid4())
    initial_tsv = temp_uuid+"ini.tsv"
    temp_tsv = temp_uuid+".tmp.tsv"
    intermediate_tsv = temp_uuid+".int.tsv"
    final_tsv = temp_uuid+".final.tsv"
    
    command = """
    dx generate_batch_inputs \
    -istage-Fk9p4Kj4yBGfpvQf85fQXJq5.reads_fastqgzs='(.*)_S(.*)_L(.*)\d*[13579]_R1(.*)' \
    -istage-Fk9p4Kj4yBGfpvQf85fQXJq5.reads_fastqgzsB='(.*)_S(.*)_L(.*)\d*[02468]_R1(.*)' \
    -istage-Fk9p4Kj4yBGfpvQf85fQXJq5.reads2_fastqgzs='(.*)_S(.*)_L(.*)\d*[13579]_R2(.*)' \
    -istage-Fk9p4Kj4yBGfpvQf85fQXJq5.reads2_fastqgzsB='(.*)_S(.*)_L(.*)\d*[02468]_R2(.*)' \
    -istage-FpGkFJj433GxvX376JyVxKpG.reads='(.*)_S(.*)_L(.*)\d*[13579]_R1(.*)' \
    -istage-FpGkFK0433Gy74J9PYJKV42y.reads='(.*)_S(.*)_L(.*)\d*[02468]_R1(.*)' \
    -istage-FpGkFK0433Gy74J9PYJKV42z.reads='(.*)_S(.*)_L(.*)\d*[13579]_R2(.*)' \
    -istage-FpGkF3j433GZg9QQ6X82Gj9V.reads='(.*)_S(.*)_L(.*)\d*[02468]_R2(.*)'; \
    \
    head -n 1 dx_batch.0000.tsv \
    > {temp_tsv} && \
    tail -n +2 dx_batch.0000.tsv | \
    awk '{{ $10 = \"[\"$10; print }}'  | awk '{{ $11 = $11\"]\"; print }}' |  \
    awk '{{ $12 = \"[\"$12; print }}'  | awk '{{ $13 = $13\"]\"; print }}' \
    >> {temp_tsv}; \
    tr -d '\r' < {temp_tsv} > {intermediate_tsv}; \
    rm {temp_tsv}
    \
    head -n 1 {intermediate_tsv} | \
    awk -F "\t" '{{ print $1"\t"$2"\t"$3"\t"$4"\t"$5"\t"$6"\t"$7"\t"$8"\t"$9"\t"$10"\t"$12"\t"$14"\t"$15"\t"$16"\t"$17"\tstage-Fk9p4Kj4yBGfpvQf85fQXJq5.sample" }}' \
    > {temp_tsv} && \
    tail -n +2 {intermediate_tsv} | \
    awk '{{ print $1"\t"$2"\t"$3"\t"$4"\t"$5"\t"$6"\t"$7"\t"$8"\t"$9"\t"$10","$11"\t"$12","$13"\t"$14"\t"$15"\t"$16"\t"$17"\t"$1 }}' \
    >> {temp_tsv}; tr -d '\r' < {temp_tsv} > {final_tsv}; \
    rm {temp_tsv}
    """.format(temp_tsv=temp_tsv, intermediate_tsv=intermediate_tsv, final_tsv=final_tsv)

    if input_directory:
        command = "dx cd {input_directory}".format(input_directory)
        subprocess.check_call(command, shell=True)
    FNULL = open(os.devnull, 'w')
    subprocess.call(command, stderr=subprocess.STDOUT, stdout=FNULL, shell=True)
    assert os.path.exists(final_tsv), "Failed to generate batch file!"
    return final_tsv


def format_relative_paths(workflow_stage_info):
    result = ""
    for stage, stage_info in sorted(workflow_stage_info.items()):
        command_option = '--stage-relative-output-folder {stage_id} "{app_name}" '\
            .format(stage_id=stage, app_name=stage_info["app_name"])
        result += command_option
    return result


def run_dias_ss_batch_file(workflow_id,
                        batch_file,
                        workflow_stage_info,
                        workflow_out_dir):
    app_relative_paths = format_relative_paths(workflow_stage_info)
    command = 'dx run --yes {workflow_id} --batch-tsv {batch_file} --destination={workflow_out_dir} {app_relative_paths}'\
        .format(workflow_id=workflow_id,
                batch_file=batch_file,
                workflow_out_dir=workflow_out_dir,
                app_relative_paths=app_relative_paths)
    subprocess.call(command, shell=True)


def make_ms_workflow_out_dir(ms_workflow_id, ss_workflow_out_dir):
    ms_workflow_name = get_object_attribute_from_object_id_or_path(ms_workflow_id, "Name")
    assert ms_workflow_name, "Workflow name not found. Aborting"
    ms_workflow_output_dir_pattern = "{ss_workflow_out_dir}{ms_workflow_name}-{date}-{index}/"
    date=get_date()

    i = 1
    while i < 100:  # < 100 runs = sanity check
        ms_workflow_output_dir = ms_workflow_output_dir_pattern\
            .format(ss_workflow_out_dir=ss_workflow_out_dir, ms_workflow_name=ms_workflow_name, date=date, index=i)
        if dx_make_workflow_dir(ms_workflow_output_dir):
            print("Using\t\t%s" % ms_workflow_output_dir)
            return ms_workflow_output_dir
        else:
            print("Skipping\t%s" % ms_workflow_output_dir)
        i += 1
    return None

def find_app_dir(workflow_output_dir, app_basename):
    if app_basename == None:
        return "/"
    command = "dx ls {workflow_output_dir} | grep {app_basename}".format(workflow_output_dir=workflow_output_dir, app_basename=app_basename)
    search_result = subprocess.check_output(command, shell=True).strip().split("\n")
    assert len(search_result) < 2, "app_basename '{app_basename}' resolved to multiple apps:\n{apps}".format(app_basename=app_basename, apps=", ".join(search_result))
    assert len(search_result) > 0, "app_basename '{app_basename}' matched no apps"
    return "".join([workflow_output_dir, search_result[0]])


def get_stage_input_file_list(app_dir, app_subdir="", filename_pattern="."):
    command = "dx ls {app_dir}{app_subdir} | grep {filename_pattern}".format(app_dir=app_dir, app_subdir=app_subdir, filename_pattern=filename_pattern)
    search_result = subprocess.check_output(command, shell=True).strip().split("\n")
    file_ids = []
    for file in search_result:
        full_path = "".join([app_dir, app_subdir, file])
        file_id = get_object_attribute_from_object_id_or_path(full_path, "ID")
        file_ids.append(file_id)
    return file_ids


def get_ms_stage_input_dict(ss_workflow_out_dir, ms_workflow_id):

    stage_input_dict = {"stage-FpPQpk8433GZz7615xq3FyvF.flagstat":{"app":"flagstat",
                                                                   "subdir": "",
                                                                   "pattern": "flagstat"},
                        "stage-FpPQpk8433GZz7615xq3FyvF.coverage":{"app":"region_coverage",
                                                                   "subdir": "",
                                                                   "pattern": "5bp.gz$",},
                        "stage-FpPQpk8433GZz7615xq3FyvF.coverage_index":{"app":"region_coverage",
                                                                          "subdir": "",
                                                                          "pattern": "5bp.gz.tbi$",},
                        "stage-Fpz3Jqj433Gpv7yQFfKz5f8g.SampleSheet":{"app":None,
                                                                      "subdir": "",
                                                                      "pattern": "SampleSheet.csv$",},
                }

    for stage_input, stage_input_info in stage_input_dict.items():
        #ss_workflow_out_dir = "/output/dias_v1.0.0_DEV-200430-1/"  # DEBUG
        input_app_dir = find_app_dir(ss_workflow_out_dir, stage_input_info["app"])
        stage_input_dict[stage_input]["file_list"] = get_stage_input_file_list(input_app_dir, app_subdir=stage_input_info["subdir"], filename_pattern=stage_input_info["pattern"])

    return stage_input_dict


def run_ss_workflow(input_dir):
    ss_workflow_id          = "project-Fkb6Gkj433GVVvj73J7x8KbV:workflow-FpG6QjQ433Gf7Gq15ZF4Vk49"
    ss_workflow_out_dir     = make_ss_workflow_out_dir(ss_workflow_id)
    ss_workflow_stage_info  = get_workflow_stage_info(ss_workflow_id)
    ss_app_out_dirs         = make_app_out_dirs(ss_workflow_stage_info,
                                                ss_workflow_id,
                                                ss_workflow_out_dir)
    ss_batch_file          = make_ss_dias_batch_file(input_dir)
    run_dias_ss_batch_file(ss_workflow_id,
                           ss_batch_file,
                           ss_workflow_stage_info,
                           ss_workflow_out_dir)
    return ss_workflow_out_dir


def run_ms_workflow(ss_workflow_out_dir):
    ms_workflow_id = "project-Fkb6Gkj433GVVvj73J7x8KbV:workflow-FpKqKP8433Gj8JbxB0433F3y"
    ms_workflow_out_dir = make_ms_workflow_out_dir(ms_workflow_id, ss_workflow_out_dir)
    ms_workflow_stage_info  = get_workflow_stage_info(ms_workflow_id)
    ms_output_dirs = make_app_out_dirs(ms_workflow_stage_info,
                                       ms_workflow_id,
                                       ms_workflow_out_dir)


    ms_stage_input_dict = get_ms_stage_input_dict(ss_workflow_out_dir, ms_workflow_id)
    

    run_wf_command = "dx run --yes {ms_workflow_id}".format(ms_workflow_id=ms_workflow_id)
    
    stage_input_str = ""
    for stage_input, stage_input_info in ms_stage_input_dict.items():
        file_links = ",".join(['{{"$dnanexus_link":"{file_id}"}}'.format(file_id=file_id) for file_id in stage_input_info["file_list"]])
        print(file_links)
        if len(stage_input_info["file_list"]) == 1:
            stage_input_str = "".join([stage_input_str, " -i{stage}='{files}' ".format(stage=stage_input, files=file_links)])
        else:
            stage_input_str = "".join([stage_input_str, " -i{stage}='[{files}]' ".format(stage=stage_input, files=file_links)])

    app_relative_paths = format_relative_paths(ms_workflow_stage_info)

    destination = " --destination={ms_workflow_out_dir} ".format(ms_workflow_out_dir=ms_workflow_out_dir)
    command = " ".join([run_wf_command, stage_input_str, app_relative_paths, destination])
    print(command)
    subprocess.check_call(command, shell=True)


parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers()

parser_s = subparsers.add_parser('single', help='single help')
parser_s.add_argument('input_dir', type=str, help='Input data directory', nargs='?', default=None)
parser_s.set_defaults(which='single')

parser_m = subparsers.add_parser('multi', help='multi help')
parser_m.add_argument('input_dir', type=str, help='Input workflow directory path')
parser_m.set_defaults(which='multi')

args = parser.parse_args()
workflow = args.which

if workflow == "single":
    ss_workflow_out_dir = run_ss_workflow(args.input_dir)
elif workflow == "multi":
    ms_workflow_out_dir = run_ms_workflow(args.input_dir)