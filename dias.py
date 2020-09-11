#!/usr/bin/python
import datetime
import subprocess
import uuid
import os
import sys
import pprint
import argparse


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


def format_relative_paths(workflow_stage_info):
    result = ""
    for stage, stage_info in sorted(workflow_stage_info.items()):
        command_option = '--stage-relative-output-folder {stage_id} "{app_name}" '\
            .format(stage_id=stage, app_name=stage_info["app_name"])
        result += command_option
    return result


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
    try:
        search_result = subprocess.check_output(command, shell=True).strip().split("\n")
    except subprocess.CalledProcessError:  # If no files found grep returns 1. This can be valid e.g. no NA12878
        search_result = []
    file_ids = []
    for file in search_result:
        full_path = "".join([app_dir, app_subdir, file])
        file_id = get_object_attribute_from_object_id_or_path(full_path, "ID")
        file_ids.append(file_id)

    return file_ids


def get_dx_cwd_project_id():
    command = 'dx env | grep -P "Current workspace\t" | awk -F "\t" \'{print $NF}\' | sed s/\'"\'//g'
    project_id = subprocess.check_output(command, shell=True).strip()
    return project_id


# Single sample apps batch

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


def make_ss_dias_batch_file(input_directory):
    # uuids for temp files to prevent collisions during parallel runs
    fastq_dict = make_fq_dict(input_directory)
    batch_uuid = str(uuid.uuid4())
    batch_tsv = batch_uuid + ".dx_batch.tsv"

    sentieon_R1_input_stage = "stage-Fk9p4Kj4yBGfpvQf85fQXJq5.reads_fastqgzs"
    sentieon_R2_input_stage = "stage-Fk9p4Kj4yBGfpvQf85fQXJq5.reads2_fastqgzs"
    sentieon_sample_input_stage = "stage-Fk9p4Kj4yBGfpvQf85fQXJq5.sample"
    fastqc_fastqs_input_stage = "stage-Fx13V7j433GjFxbX2XxzYJVY.fastqs"
    id_suffix = " ID"

    headers = ["batch ID",
               sentieon_R1_input_stage,
               sentieon_R2_input_stage,
               sentieon_sample_input_stage,
               fastqc_fastqs_input_stage,
               sentieon_R1_input_stage + id_suffix,
               sentieon_R2_input_stage + id_suffix,
               fastqc_fastqs_input_stage + id_suffix]

    batch_file_lines = []
    header_line = "\t".join(headers)
    batch_file_lines.append(header_line)

    for sample, reads in sorted(fastq_dict.items()):
        assert(len(reads["R1"]) == len(reads["R2"])), "Mismatched number of R1/R2 fastqs for {}".format(sample)
        r_1   = "[" + ",".join(reads["R1"]) + "]"
        r_2   = "[" + ",".join(reads["R2"]) + "]"
        r_all = "[" + ",".join([",".join(reads["R1"]), ",".join(reads["R2"])]) + "]"
        data_fields = [sample, "-","-",sample,"-",r_1,r_2,r_all]
        data_line = "\t".join(data_fields)
        batch_file_lines.append(data_line)

    with open(batch_tsv, "w") as batch_fh:
        for line in batch_file_lines:
            batch_fh.write("%s\n" % line)
    
    assert os.path.exists(batch_tsv), "Failed to generate batch file!"
    return batch_tsv


def make_fq_dict(path):

    command = "dx find data --path {path} --name *fastq.gz --brief".format(path=path)
    fastq_id_list = subprocess.check_output(command, shell=True).strip().split("\n")
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
        
        assert read_num, "Unable to determine read number (R1 or R2) for fastq {}".format(fastq_filename)

        # Make a new dict entry for sample if not present
        fastq_dict.setdefault(sample_id, {"R1":[],
                                          "R2":[]})

        # Add fastq filename and file_id to appropriate place in dict. 
        # We add both id and name because we need to sort by name later
        fastq_dict[sample_id].setdefault(read_num, []).append((fastq_filename, fastq_file_id))

    # Sort fastq lists so that the fastq at pos n in R1 list
    # is paired with the fastq at pos n in R2 list
    # Once the sort is complete we remove the filename from the dict
    # since it was only there to enable the sort
    for sample in fastq_dict:
    	for read in ["R1", "R2"]:
    		# sort tuple on first element i.e. filename
    		# retain only file id i.e. second element
    		sorted_fastq_list = \
    			[x[1] for x in sorted(fastq_dict[sample][read], 
    								  key=lambda x: x[0])]
    		fastq_dict[sample][read] = sorted_fastq_list
    
    return fastq_dict


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


def run_ss_workflow(input_dir):
    assert input_dir.startswith("/"), "Input directory must be full path (starting at /)"
    ss_workflow_id          = "project-Fkb6Gkj433GVVvj73J7x8KbV:workflow-Fx13Gj8433GpvFf4Kg0535bK"
    ss_workflow_out_dir     = make_ss_workflow_out_dir(ss_workflow_id)
    ss_workflow_stage_info  = get_workflow_stage_info(ss_workflow_id)
    ss_app_out_dirs         = make_app_out_dirs(ss_workflow_stage_info,
                                                ss_workflow_out_dir)
    ss_batch_file          = make_ss_dias_batch_file(input_dir)
    run_dias_ss_batch_file(ss_workflow_id,
                           ss_batch_file,
                           ss_workflow_stage_info,
                           ss_workflow_out_dir)
    return ss_workflow_out_dir


# Multi sample apps

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


def get_ms_stage_input_dict(ss_workflow_out_dir):

    stage_input_dict = {"stage-FpPQpk8433GZz7615xq3FyvF.flagstat":{"app":"flagstat",
                                                                   "subdir": "",
                                                                   "pattern": "flagstat$"},
                        "stage-FpPQpk8433GZz7615xq3FyvF.coverage":{"app":"region_coverage",
                                                                   "subdir": "",
                                                                   "pattern": "5bp.gz$",},
                        "stage-FpPQpk8433GZz7615xq3FyvF.coverage_index":{"app":"region_coverage",
                                                                         "subdir": "",
                                                                         "pattern": "5bp.gz.tbi$",},
                        "stage-Fpz3Jqj433Gpv7yQFfKz5f8g.SampleSheet":{"app":None,
                                                                      "subdir": "",
                                                                      "pattern": "SampleSheet.csv$",},
                        "stage-Fq1BPKj433Gx3K4Y8J35j0fv.query_vcf":{"app":"sentieon-dnaseq",
                                                                    "subdir": "",
                                                                    "pattern": "NA12878_markdup_recalibrated_Haplotyper.vcf.gz$",},                }

    for stage_input, stage_input_info in stage_input_dict.items():
        #ss_workflow_out_dir = "/output/dias_v1.0.0_DEV-200430-1/"  # DEBUG
        input_app_dir = find_app_dir(ss_workflow_out_dir, stage_input_info["app"])
        stage_input_dict[stage_input]["file_list"] = get_stage_input_file_list(input_app_dir, app_subdir=stage_input_info["subdir"], filename_pattern=stage_input_info["pattern"])

    return stage_input_dict


def make_ms_dias_batch_file(ms_stage_input_dict, ss_workflow_out_dir, ms_workflow_out_dir):
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

        # making a square bracketed comma separated list if multiple input files 
        elif len(ms_stage_input_dict[stage_input]["file_list"]) > 1:
            # Square bracketed csv list
            file_ids = "[{file_ids}]".format(file_ids = ",".join([file_id for file_id in ms_stage_input_dict[stage_input]["file_list"]]))
            values.append("")  # No need to provide file name in batch file
            values.append(file_ids)

    # Write the file content
    with open(batch_filename, "w") as b_fh:
        for line in [headers, values]:
            tsv_line = "\t".join(line) + "\n"
            b_fh.write(tsv_line)

    return batch_filename


def run_ms_workflow(ss_workflow_out_dir):
    assert ss_workflow_out_dir.startswith("/"), "Input directory must be full path (starting at /)"
    ms_workflow_id = "project-Fkb6Gkj433GVVvj73J7x8KbV:workflow-FpKqKP8433Gj8JbxB0433F3y"
    ms_workflow_out_dir = make_ms_workflow_out_dir(ms_workflow_id, ss_workflow_out_dir)
    ms_workflow_stage_info  = get_workflow_stage_info(ms_workflow_id)
    ms_output_dirs = make_app_out_dirs(ms_workflow_stage_info,
                                       ms_workflow_out_dir)

    ms_stage_input_dict = get_ms_stage_input_dict(ss_workflow_out_dir)
    ms_batch_file = make_ms_dias_batch_file(ms_stage_input_dict,ss_workflow_out_dir, ms_workflow_out_dir)

    run_wf_command = "dx run --yes {ms_workflow_id} --batch-tsv={ms_batch_file}".format(ms_workflow_id=ms_workflow_id,ms_batch_file=ms_batch_file)

    app_relative_paths = format_relative_paths(ms_workflow_stage_info)

    destination = " --destination={ms_workflow_out_dir} ".format(ms_workflow_out_dir=ms_workflow_out_dir)

    command = " ".join([run_wf_command, app_relative_paths, destination])
    subprocess.check_call(command, shell=True)

    return ms_workflow_out_dir


# MultiQC

def run_multiqc_app(ms_workflow_out_dir):
    assert ms_workflow_out_dir.startswith("/"), "Input directory must be full path (starting at /)"
    
    mqc_applet_id = "project-Fkb6Gkj433GVVvj73J7x8KbV:applet-Fx7vgBQ433GpF2Xy4xQ55j6P"
    mqc_config_file = "project-Fkb6Gkj433GVVvj73J7x8KbV:file-Fx32KXj433GkVgk83fv6Zjxx"
    
    project_id = get_dx_cwd_project_id()
    path_dirs = [x for x in ms_workflow_out_dir.split("/") if x]
    assert path_dirs[-3] == "output"
    assert "single" in path_dirs[-2]
    assert "multi" in path_dirs[-1]
    ss_for_multiqc = path_dirs[-2]
    ms_for_multiqc = path_dirs[-1]

    mqc_applet_name = get_object_attribute_from_object_id_or_path(mqc_applet_id, "Name")
    mqc_applet_out_dir = "".join([ms_workflow_out_dir,mqc_applet_name])

    dx_make_workflow_dir(mqc_applet_out_dir)

    command = "dx run {applet_id} --yes -ieggd_multiqc_config_file='{mqc_config_file}' -iproject_for_multiqc='{project_id}' -iss_for_multiqc='{ss_for_multiqc}' -ims_for_multiqc='{ms_for_multiqc}' --destination='{mqc_out_dir}'"
    command = command.format(applet_id=mqc_applet_id, 
                             mqc_config_file=mqc_config_file, 
                             project_id=project_id, 
                             ss_for_multiqc=ss_for_multiqc, 
                             ms_for_multiqc=ms_for_multiqc,
                             mqc_out_dir=mqc_applet_out_dir)

    subprocess.check_call(command, shell=True)

    return mqc_applet_out_dir


# vcf2xls

def make_vcf2xls_batch_file(input_directory):
    # uuids for temp files to prevent collisions during parallel runs

    # Input dir is multi sample output dir. We want input files from the parent single sample dir
    assert input_directory.startswith("/"), "Input directory must be full path (starting at /)"
    command = "dx cd {input_directory}; dx cd ..".format(input_directory=input_directory)
    subprocess.check_call(command, shell=True)
    batch_uuid = str(uuid.uuid4())
    batch_tsv = "{batch_uuid}.0000.tsv".format(batch_uuid=batch_uuid)

    sample = "(.*)"

    command = """
    dx generate_batch_inputs \
    -iannotated_vcf='{sample}_markdup(.*)annotated.vcf$' \
    -iraw_vcf='{sample}_markdup(.*)Haplotyper.vcf.gz$$' \
    -isample_coverage_file='{sample}_markdup(.*)nirvana_20(.*)_5bp.gz$' \
    -isample_coverage_index='{sample}_markdup(.*)nirvana_20(.*)_5bp.gz.tbi$' \
    -iflagstat_file='{sample}_markdup.flagstat' \
    -o {batch_uuid}""".format(sample=sample, batch_uuid=batch_uuid)

    FNULL = open(os.devnull, 'w')
    subprocess.call(command, stderr=subprocess.STDOUT, stdout=FNULL, shell=True)
    assert os.path.exists(batch_tsv), "Failed to generate batch file!"
    return batch_tsv


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


def run_vcf2xls_app(ms_workflow_out_dir, reanalysis_dict=None):
    # Static
    vcf2xls_applet_id = "project-Fkb6Gkj433GVVvj73J7x8KbV:applet-Fx2Xgbj433GkgZ977ZFv8vZP"
    genepanels_file = "project-Fkb6Gkj433GVVvj73J7x8KbV:file-Fq3yY48433GxY9VQ9ZZ9ZfqX"
    bioinformatic_manifest = "project-Fkb6Gkj433GVVvj73J7x8KbV:file-FvyX44j433Gz74z60Vg0QgkG"
    exons_nirvana = "project-Fkb6Gkj433GVVvj73J7x8KbV:file-Fq18Yp0433GjB7172630p9Yv"
    nirvana_genes2transcripts = "project-Fkb6Gkj433GVVvj73J7x8KbV:file-FxJ5FZ0433GvF4X84Kvj3GKB"

    # Dynamic - run dependent
    command = "dx find data --path {ms_workflow_out_dir}expected_depth_v1.1.2/ --name *gz --brief".format(ms_workflow_out_dir=ms_workflow_out_dir)
    runfolder_coverage_file = subprocess.check_output(command, shell=True).strip()

    command = "dx find data --path {ms_workflow_out_dir}expected_depth_v1.1.2/ --name *gz.tbi --brief".format(ms_workflow_out_dir=ms_workflow_out_dir)
    runfolder_coverage_index = subprocess.check_output(command, shell=True).strip()

    vcf2xls_applet_name = get_object_attribute_from_object_id_or_path(vcf2xls_applet_id, "Name")
    vcf2xls_applet_out_dir = "".join([ms_workflow_out_dir,vcf2xls_applet_name])

    batch_file = make_vcf2xls_batch_file(ms_workflow_out_dir)
    if reanalysis_dict:
        batch_file = make_reanalysis_batch_file(batch_file, reanalysis_dict)
    dx_make_workflow_dir(vcf2xls_applet_out_dir)

    command = "dx run {applet_id} --yes --destination='{vcf2xls_applet_out_dir}' --batch-tsv='{batch_file}' -irunfolder_coverage_file='{runfolder_coverage_file}' -irunfolder_coverage_index='{runfolder_coverage_index}' -igenepanels_file='{genepanels_file}' -ibioinformatic_manifest='{bioinformatic_manifest}' -iexons_nirvana='{exons_nirvana}' -inirvana_genes2transcripts='{nirvana_genes2transcripts}' "

    command = command.format(applet_id=vcf2xls_applet_id, 
                             vcf2xls_applet_out_dir=vcf2xls_applet_out_dir,
                             batch_file=batch_file,
                             runfolder_coverage_file=runfolder_coverage_file,
                             runfolder_coverage_index=runfolder_coverage_index,
                             genepanels_file=genepanels_file,
                             bioinformatic_manifest=bioinformatic_manifest,
                             exons_nirvana=exons_nirvana,
                             nirvana_genes2transcripts=nirvana_genes2transcripts
                             )

    subprocess.check_call(command, shell=True)

    return vcf2xls_applet_out_dir


def run_reanalysis(input_dir, reanalysis_list):
    reanalysis_dict = {}

    with open(reanalysis_list) as r_fh:
        for line in r_fh:
            fields = line.strip().split("\t")
            assert len(fields) == 2, "Unexpected number of fields in reanalysis_list. File must contain one tab separated sample/panel combination per line"
            sample, panel = fields
            reanalysis_dict.setdefault(sample, set()).add(panel)

    run_vcf2xls_app(input_dir, reanalysis_dict)


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    parser_s = subparsers.add_parser('single', help='single help')
    parser_s.add_argument('input_dir', type=str, help='Input data directory path')
    parser_s.set_defaults(which='single')

    parser_m = subparsers.add_parser('multi', help='multi help')
    parser_m.add_argument('input_dir', type=str, help='A single sample workflow output directory path')
    parser_m.set_defaults(which='multi')

    parser_q = subparsers.add_parser('qc', help='multiqc help')
    parser_q.add_argument('input_dir', type=str, help='A multi sample workflow output directory path')
    parser_q.set_defaults(which='qc')

    parser_r = subparsers.add_parser('reports', help='reports help')
    parser_r.add_argument('input_dir', type=str, help='A multi sample workflow output directory path')
    parser_r.set_defaults(which='reports')

    parser_r = subparsers.add_parser('reanalysis', help='reanalysis help')
    parser_r.add_argument('input_dir', type=str, help='A multi sample workflow output directory path')
    parser_r.add_argument('reanalysis_list', type=str, help='Tab delimited file containg sample and panel for reanalysis. One sample/panel combination per line')
    parser_r.set_defaults(which='reanalysis')


    args = parser.parse_args()
    workflow = args.which
    assert workflow, "Please specify a subcommand"
    if args.input_dir and not args.input_dir.endswith("/"):
        args.input_dir = args.input_dir + "/"

    if workflow == "single":
        ss_workflow_out_dir = run_ss_workflow(args.input_dir)
    elif workflow == "multi":
        ms_workflow_out_dir = run_ms_workflow(args.input_dir)
    elif workflow == "qc":
        mqc_applet_out_dir = run_multiqc_app(args.input_dir)
    elif workflow == "reports":
        reports_out_dir = run_vcf2xls_app(args.input_dir)
    elif workflow == "reanalysis":
        reports_out_dir = run_reanalysis(args.input_dir, args.reanalysis_list)

if __name__ == "__main__":
    main()
