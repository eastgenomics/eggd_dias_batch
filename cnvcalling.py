#!/usr/bin/python
from ast import excepthandler
import sys
import subprocess
import dxpy

from general_functions import (
    get_dx_cwd_project_name,
    get_object_attribute_from_object_id_or_path,
    dx_make_workflow_dir,
    find_app_dir,
    get_stage_input_file_list,
    get_date
)


def find_files(project_name, app_dir, pattern="."):
    """Searches for files ending in provided pattern (bam/bai) in a
    given path (single).

    Args:
        app_dir (str): single path including directory to output app.
        pattern (str): searchs for files ending in given pattern.
        Defaults to ".".
        project_name (str): The project name on DNAnexus

    Returns:
        search_result: list containing files ending in given pattern
        of every sample processed in single.
    """
    projectID  = list(dxpy.bindings.search.find_projects(name=project_name))[0]['id']
    # the pattern is usually "-E 'pattern'" and we dont want the -E part
    pattern = pattern.split('-E ')[1].replace("'", "")
    search_result = []

    try:
        for file in dxpy.bindings.search.find_data_objects(
            project=projectID, folder=app_dir,classname="file",
            name=pattern, name_mode="regexp", describe=True
            ):
            search_result.append(file["describe"]["name"])
    except ValueError:
        print('Could not files {} in {}'.format(
              pattern,app_dir
            ))

    return search_result


# Run-level CNV calling of samples that passed QC


def run_cnvcall_app(ss_workflow_out_dir, dry_run, assay_config, assay_id, sample_list):
    """Sets off the CNV calling app.

    Args:
        ss_workflow_out_dir (str): path to single output
        dry_run (str): optional boolean whether this is a dry/test run
        assay_config (variable): assay config file containing all variables
        sample_list (file): optional file containg list of samples to exclude

    Returns:
        app_out_dir: path to where the CNV calling output will be
    """
    # Find project to create jobs and outdirs in
    project_name = get_dx_cwd_project_name()

    # Make sure path provided is an actual ss workflow output folder
    assert ss_workflow_out_dir.startswith("/"), (
        "Input directory must be full path (starting at /)")
    path_dirs = [x for x in ss_workflow_out_dir.split("/") if x]
    ss_for_multiqc = [ele for ele in path_dirs if "single" in ele]
    assert ss_for_multiqc != [], (
        "Path '{}' is not an accepted directory, "
        "must contain 'single'".format(ss_workflow_out_dir)
    )
    ss_for_multiqc = ss_for_multiqc[0]

    # Find the app name and create an output folder for it under ss
    app_name = get_object_attribute_from_object_id_or_path(
        assay_config.cnvcall_app_id, "Name"
    )

    app_output_dir_pattern = "{ss_workflow_out_dir}/{app_name}-{assay}-{date}-{index}/"
    date = get_date()

    # when creating the new folder, check if the folder already exists
    # increment index until it works or reaches 100
    i = 1
    while i < 100:  # < 100 runs = sanity check
        app_output_dir = app_output_dir_pattern.format(
            ss_workflow_out_dir=ss_workflow_out_dir,
            app_name=app_name, assay=assay_id, date=date, index=i
        )

        if dx_make_workflow_dir(app_output_dir):
            print("Using\t\t%s" % app_output_dir)
            return app_output_dir
        else:
            print("Skipping\t%s" % app_output_dir)

        i += 1

    # app_out_dir = "".join([ss_workflow_out_dir, app_name])
    # dx_make_workflow_dir(app_out_dir)

    # Find bam and bai files from sentieon folder
    bambi_files = []
    folder_path = find_app_dir(ss_workflow_out_dir, assay_config.cnvcalling_input_dict["app"])
    extensions = assay_config.cnvcalling_input_dict["patterns"]
    for ext in extensions:
        bambi_files.extend(find_files(project_name, folder_path, ext))

    # Read in list of samples that did NOT PASS QC
    if sample_list is None:
        sample_names = []
    else:
        sample_names = []
        # parse sample exclusion file
        with open(sample_list) as fh:
            for line in fh:  # line can be a sample name or sample tab panel name
                sample_names.append(line.strip().split("\t")[0])

    # Remove bam/bai files of QC faild samples
    sample_bambis = [x for x in bambi_files if x.split('_')[0] not in sample_names]

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
        "-irun_name='{}' "
        "--destination='{}'"
    ).format(
        assay_config.cnvcall_app_id,
        assay_config.cnvcalling_fixed_inputs["gatk_docker"],
        assay_config.cnvcalling_fixed_inputs["interval_list"],
        assay_config.cnvcalling_fixed_inputs["annotation_tsv"],
        file_ids,
        project_name, app_output_dir
    )

    if dry_run is True:
        print("Final cmd ran: {}".format(command))
    else:
        subprocess.call(command, shell=True)

    return app_output_dir
