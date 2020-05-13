import datetime
import subprocess
import uuid
import os


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


def describe_workflow(workflow_id):
    command = "dx describe {workflow_id}".format(
        workflow_id=workflow_id)

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


def get_object_name_from_object_id(workflow_id):
    workflow_description = describe_workflow(workflow_id)

    for line in workflow_description:
        if line.startswith("Name "):
            workflow_name = line.split(" ")[-1]
            return workflow_name
    return None


def make_workflow_out_dir(workflow_id):
    workflow_name = get_object_name_from_object_id(workflow_id)
    assert workflow_name, "Workflow name not found. Aborting"
    workflow_output_dir_pattern = "/output/{workflow_name}-{date}-{index}/"

    i = 1
    while i < 100:  # < 100 runs = sanity check
        workflow_output_dir = workflow_output_dir_pattern\
            .format(workflow_name=workflow_name, date=get_date(), index=i)
        if dx_make_workflow_dir(workflow_output_dir):
            print("Using\t\t%s" % workflow_output_dir)
            return workflow_output_dir
        else:
            print("Skipping\t%s" % workflow_output_dir)
        i += 1
    return None


def get_workflow_stage_info(workflow_id):
    workflow_description = describe_workflow(workflow_id)

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
            app_name = get_object_name_from_object_id(app_id)

            stages[stage] = {"app_id": app_id,
                             "app_name": app_name}
            previous_line_is_stage = False

        else:
            previous_line_is_stage = False

    return stages


def make_app_out_dirs(workflow_stage_info,
                      workflow_id,
                      workflow_output_dir):
    for stage, stage_info in sorted(workflow_stage_info.items()):
        app_out_dir = "{workflow_output_dir}{app_name}"\
            .format(workflow_output_dir=workflow_output_dir,
                    app_name=stage_info["app_name"])
        # mkdir with -p so no error if multiples of same app try to make
        # multiple dirs e.g. fastqc
        command = "dx mkdir -p {app_out_dir}"\
            .format(app_out_dir=app_out_dir)

        subprocess.check_output(command, shell=True)
    return True


def make_dias_batch_file():
    # uuids for temp files to prevent collisions during parallel runs
    temp_uuid = str(uuid.uuid4())
    initial_tsv = temp_uuid+"ini.tsv"
    temp_tsv = temp_uuid+".tmp.tsv"
    intermediate_tsv = temp_uuid+".int.tsv"
    final_tsv = temp_uuid+".final.tsv"
    command = """
    dx generate_batch_inputs \
    -istage-Fk9p4Kj4yBGfpvQf85fQXJq5.reads_fastqgzs='(.*)_S(.*)_L(.*)\d*[13579]_R1(.*)' \
    -istage-Fk9p4Kj4yBGfpvQf85fQXJq5.reads_fastqgzsB='(.*)_S(.*)_L(.*)\d[02468]_R1(.*)' \
    -istage-Fk9p4Kj4yBGfpvQf85fQXJq5.reads2_fastqgzs='(.*)_S(.*)_L(.*)\d[13579]_R2(.*)' \
    -istage-Fk9p4Kj4yBGfpvQf85fQXJq5.reads2_fastqgzsB='(.*)_S(.*)_L(.*)\d[02468]_R2(.*)' \
    -istage-FpGkFJj433GxvX376JyVxKpG.reads='(.*)_S(.*)_L(.*)\d*[13579]_R1(.*)' \
    -istage-FpGkFK0433Gy74J9PYJKV42y.reads='(.*)_S(.*)_L(.*)\d[02468]_R1(.*)' \
    -istage-FpGkFK0433Gy74J9PYJKV42z.reads='(.*)_S(.*)_L(.*)\d[13579]_R2(.*)' \
    -istage-FpGkF3j433GZg9QQ6X82Gj9V.reads='(.*)_S(.*)_L(.*)\d[02468]_R2(.*)'; \
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

    FNULL = open(os.devnull, 'w')
    subprocess.call(command, stderr=subprocess.STDOUT, stdout=FNULL, shell=True)
    assert os.path.exists(final_tsv), "Failed to generate batch file!"
    return final_tsv


def format_relative_paths(app_out_dirs):
    result = ""
    for stage, stage_info in sorted(workflow_stage_info.items()):
        command_option = '--stage-relative-output-folder {stage_id} "{app_name}" '\
            .format(stage_id=stage, app_name=stage_info["app_name"])
        result += command_option
    return result


def run_dias_batch_file(workflow_id,
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


workflow_id         = "workflow-FpG6QjQ433Gf7Gq15ZF4Vk49"
workflow_out_dir    = make_workflow_out_dir(workflow_id)
workflow_stage_info = get_workflow_stage_info(workflow_id)
app_out_dirs        = make_app_out_dirs(workflow_stage_info,
                                        workflow_id,
                                        workflow_out_dir)
batch_file          = make_dias_batch_file()
run_dias_batch_file(workflow_id,
                    batch_file,
                    workflow_stage_info,
                    workflow_out_dir)
