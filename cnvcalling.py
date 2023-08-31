#!/usr/bin/python
import subprocess

from general_functions import (
    dx_get_project_id,
    dx_get_object_name,
    get_object_attribute_from_object_id_or_path,
    find_app_dir,
    make_app_output_dir,
    find_files
)

# Run-level CNV calling of samples that passed QC


def run_cnvcall_app(ss_workflow_out_dir, dry_run, assay_config, assay_id, excluded_sample_list):
    """Sets off the CNV calling app.

    Args:
        ss_workflow_out_dir (str): path to single output
        dry_run (str): optional boolean whether this is a dry/test run
        assay_config (variable): assay config file containing all variables
        excluded_sample_list (file): optional file containg list of samples to exclude

    Returns:
        app_out_dir: path to where the CNV calling output will be
    """
    # Find project to create jobs and outdirs in
    project_id = dx_get_project_id()
    project_name = dx_get_object_name(project_id)
    print("Jobs will be set off in project {}".format(project_name))

    # Check that provided input directory is an absolute path
    assert ss_workflow_out_dir.startswith("/output/"), (
        "Input directory must be full path (starting with /output/)")

    # Find the app name and create an output folder for it under ss
    app_name = get_object_attribute_from_object_id_or_path(
        assay_config.cnvcall_app_id, "Name"
    )
    app_output_dir = make_app_output_dir(assay_config.cnvcall_app_id, ss_workflow_out_dir, app_name, assay_id)

    # Find bam and bai files from sentieon folder
    bambi_files = []
    folder_path = find_app_dir(ss_workflow_out_dir, assay_config.cnvcalling_input_dict["app"])
    extensions = assay_config.cnvcalling_input_dict["patterns"]
    for ext in extensions:
        bambi_files.extend(find_files(project_name, folder_path, ext))

    # Read in list of samples that did NOT PASS QC
    excluded_sample_names = []
    # parse sample exclusion file
    with open(excluded_sample_list) as fh:
        for line in fh:  
            # line can be a sample name or sample tab panel name
            # remove file extensions after first "_"
            excluded_sample_names.append(line.strip().split("\t")[0].split('_')[0])

    # Remove bam/bai files of excluded (QC failed) samples
    sample_bambis = [x for x in bambi_files if x.split('_')[0] not in excluded_sample_names]
    print(
        "{} out of {} samples were excluded from CNV calling".format(
            (len(bambi_files) - len(sample_bambis)) / 2, len(bambi_files) / 2)
        )
    if len(sample_bambis) < 60:
        print("Fewer than 30 samples suitable for CNV calling, \n \
        which is below the threshold for optimal performance")

    # Find the file-IDs of the passed bam/bai samples
    file_ids = ""

    for file in sample_bambis:
        full_path = "".join([folder_path, file])
        file_id = get_object_attribute_from_object_id_or_path(full_path, "ID")
        file_ids += " -ibambais=" + file_id

    command = (
        "dx run {} --yes --ignore-reuse -iGATK_docker='{}' "
        "-iinterval_list='{}' -iannotation_tsv='{}' {} "
        "-idebug_fail_start=False -idebug_fail_end=False "
        "-irun_name='{}' --destination='{}' "
        "--extra-args='{"timeout": 10800000}' "
        "--extra-args='{"systemRequirements": {"*": {"instanceType": "mem2_ssd1_v2_x8"}}}'"
    ).format(
        assay_config.cnvcall_app_id,
        assay_config.cnvcalling_fixed_inputs["gatk_docker"],
        assay_config.cnvcalling_fixed_inputs["interval_list"],
        assay_config.cnvcalling_fixed_inputs["annotation_tsv"],
        file_ids,
        project_name, app_output_dir
    )

    # upload excluded regions file
    excluded_list_path = app_output_dir + "/" + "excluded_list.tsv"
    cmd = "dx upload {} --path {}".format(excluded_sample_list, excluded_list_path)

    if dry_run is True:
        print("Created output dir: {}".format(app_output_dir))
        print("Final cmd ran: {}".format(command))
        print("Deleting '{}' as part of the dry-run".format(app_output_dir))
        delete_folders_cmd = "dx rm -r {}".format(app_output_dir)
        subprocess.call(delete_folders_cmd, shell=True)

    else:
        subprocess.call(command, shell=True)
        subprocess.check_output(cmd, shell=True)

    return app_output_dir
