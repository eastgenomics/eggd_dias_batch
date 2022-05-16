#!/usr/bin/python

from collections import defaultdict
import datetime
import os
import subprocess
import uuid
import dxpy

from packaging import version

# Generic functions


def get_date():
    """ Get the date in YYMMDD format

    Returns:
        str: String of date in YYMMDD format
    """

    date = datetime.datetime.now()
    return date.strftime("%y%m%d")


def dx_make_workflow_dir(dx_dir_path):
    """ Create workflow directories

    Args:
        dx_dir_path (str): String of the DNAnexus path to create

    Returns:
        bool: Bool to check whether creation of folders was successful
    """

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
    """ Describe DNAnexus object

    Args:
        object_id_or_path (str): DNAnexus object id or path

    Returns:
        str: Description of object
    """

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
    """ Get specific attribute from DNAnexus description

    Args:
        object_id_or_path (str): DNAnexus object id or path
        attribute (str): Attribute to extract from description

    Returns:
        str: Value of attribute
    """

    workflow_description = describe_object(object_id_or_path)

    for line in workflow_description:
        if line.startswith("{attribute} ".format(attribute=attribute)):
            attribute_value = line.split(" ")[-1]
            return attribute_value
    return None


def get_workflow_stage_info(workflow_id):
    """ Get the workflow stage info i.e. stage id, app id and app name

    Args:
        workflow_id (str): Workflow id

    Returns:
        dict: Dict of stage id to app info
    """

    workflow_description_json = dxpy.describe(workflow_id.split(":")[1])

    stages = {}

    # go through the workflow json description and select stage,
    # app_id and app_name
    for stage in workflow_description_json['stages']:
        # gather app id and app name of the stage
        app_id = stage['executable']
        app_name = dxpy.describe(app_id)['name']
        stages[stage['id']] = {
            "app_id": app_id, "app_name": app_name
        }

    return stages


def make_app_out_dirs(workflow_stage_info, workflow_output_dir):
    """ Create directories for the apps

    Args:
        workflow_stage_info (str): Dict of stage to app info
        workflow_output_dir (str): Output directory for the workflow

    Returns:
        dict: Dict of stage id to app output directory
    """

    out_dirs = {}

    for stage, stage_info in sorted(workflow_stage_info.items()):
        # full path of the app output folder
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
    """ Add specific app output directory to final command line

    Args:
        workflow_stage_info (dict): Dict containing the stage to app info

    Returns:
        str: String containing the DNAnexus option to specify apps output directory
    """

    result = ""
    for stage_id, stage_info in sorted(workflow_stage_info.items()):
        command_option = '--stage-relative-output-folder {} "{}" '.format(
            stage_id, stage_info["app_name"]
        )
        result += command_option
    return result


def find_app_dir(workflow_output_dir, app_basename):
    """ Find app directory

    Args:
        workflow_output_dir (str): Workflow output directory
        app_basename (str): App name without version

    Returns:
        str: Full path of app directory
    """

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
    """ Get the file ids for a given app directory

    Args:
        app_dir (str): App directory
        app_subdir (str, optional): App sub-directory. Defaults to "".
        filename_pattern (str, optional): Pattern used to find files. Defaults to ".".

    Returns:
        list: List of file ids for the app folder
    """

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
    """ Return project id using dx env

    Returns:
        str: DNAnexus project id
    """

    command = (
        'dx env | grep -P "Current workspace\t" | '
        'awk -F "\t" \'{print $NF}\' | sed s/\'"\'//g'
    )
    project_id = subprocess.check_output(command, shell=True).strip()
    return project_id


def get_dx_cwd_project_name():
    """ Return project name using dx env

    Returns:
        str: DNAnexus project name
    """

    command = (
        'dx env | grep -P "Current workspace name" | '
        'awk -F "\t" \'{print $NF}\' | sed s/\'"\'//g'
    )

    project_name = subprocess.check_output(command, shell=True).strip()
    return project_name


def get_sample_ids_from_sample_sheet(sample_sheet_path):
    """ Return list of samples from the sample sheet

    Args:
        sample_sheet_path (str): Path to the sample sheet

    Returns:
        list: List of samples
    """

    sample_ids = []
    cmd = "dx cat {}".format(sample_sheet_path)
    sample_sheet_content = subprocess.check_output(cmd, shell=True).split("\n")

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
                    # get the sample ids using the header position
                    if line[sample_id_pos] != "NA12878":
                        sample_ids.append(line[sample_id_pos])

                index += 1
            else:
                # Use [Data] as a way to identify when we reached data
                if line.startswith("[Data]"):
                    data = True

    return sample_ids


def make_workflow_out_dir(workflow_id, assay_id, workflow_out_dir="/output/"):
    """ Return the workflow output dir so that it is not duplicated when run

    Args:
        workflow_id (str): Workflow id
        workflow_out_dir (str, optional): Where the workflow folder will be created. Defaults to "/output/".

    Returns:
        str: Workflow out dir
    """

    workflow_name = get_object_attribute_from_object_id_or_path(
        workflow_id, "Name"
    )
    assert workflow_name, "Workflow name not found. Aborting"

    workflow_dir = "{}{}".format(workflow_out_dir, workflow_name)

    workflow_output_dir_pattern = "{workflow_dir}-{assay}-{date}-{index}/"
    date = get_date()

    # when creating the new folder, check if the folder already exists
    # increment index until it works or reaches 100
    i = 1
    while i < 100:  # < 100 runs = sanity check
        workflow_output_dir = workflow_output_dir_pattern.format(
            workflow_dir=workflow_dir, assay=assay_id, date=date, index=i
        )

        if dx_make_workflow_dir(workflow_output_dir):
            print("Using\t\t%s" % workflow_output_dir)
            return workflow_output_dir
        else:
            print("Skipping\t%s" % workflow_output_dir)

        i += 1

    return None


def get_stage_inputs(ss_workflow_out_dir, stage_input_dict):
    """ Return dict with sample2stage2files

    Args:
        ss_workflow_out_dir (str): Directory of single workflow
        stage_input_dict (dict): Dict of stage2app

    Returns:
        dict: Dict of sample2stage2file_list
    """

    # Allows me to not have to check if a key exists before creating an entry in the dict
    # Example: dict[entry][sub-entry][list-entry].append(ele)
    dict_res = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    # type_input can be either "multi" or a sample id
    for type_input in stage_input_dict:
        # find the inputs for each stage using given app/pattern
        for stage_input, stage_input_info in stage_input_dict[type_input].items():
            input_app_dir = find_app_dir(
                ss_workflow_out_dir, stage_input_info["app"]
            )
            inputs = get_stage_input_file_list(
                input_app_dir,
                app_subdir=stage_input_info["subdir"],
                filename_pattern=stage_input_info["pattern"].format(type_input)
            )
            dict_res[type_input][stage_input]["file_list"] = inputs

    return dict_res


def prepare_batch_writing(
    stage_input_dict, type_workflow, assay_config, workflow_specificity={}
):
    """ Return headers and values for the batch file writing

    Args:
        stage_input_dict (dict): Dict of sample2stage2file_list
        type_workflow (str): String equal to either multi or reports
        workflow_specificity (dict, optional): For the reports, add the dynamic files to headers + values. Defaults to {}.

    Returns:
        tuple: Tuple of headers and values
    """
    batch_headers = []
    batch_values = []

    for type_input in stage_input_dict:
        stage_data = stage_input_dict[type_input]
        headers = ["batch ID"]
        values = []

        # multi needs the happy static value
        if type_workflow == "multi":
            values.append("multi")
            # Hap.py - static values
            headers.append(assay_config.happy_stage_prefix)
            values.append("NA12878")

        elif type_workflow == "reports":
            # renaming type_input for comprehension in this elif
            sample_id = type_input

            if sample_id.startswith("NA12878"):
                continue

            values.append(sample_id)

            # get the index for the coverage report that needs to be created
            coverage_reports = find_previous_coverage_reports(sample_id)
            index = get_next_index(coverage_reports)

            # add the name param to athena
            headers.append("{}.name".format(assay_config.athena_stage_id))
            # add the value of name to athena
            values.append("{}_{}".format(sample_id, index))

        # add the dynamic files to the headers and values
        for stage, file_id in workflow_specificity.items():
            headers.append(stage)  # col for file name
            values.append(file_id)

        # For each stage add the column header and the values in that column
        for stage_input in stage_data:
            if len(stage_data[stage_input]["file_list"]) == 0:
                continue

            headers.append(stage_input)  # col for file name
            headers.append(" ".join([stage_input, "ID"]))  # col for file ID

            # One file in file list - no need to merge into array
            if len(stage_data[stage_input]["file_list"]) == 1:
                file_ids = stage_data[stage_input]["file_list"][0]
                values.append("")  # No need to provide file name in batch file
                values.append(file_ids)

            # make a square bracketed comma separated list if multiple input files
            elif len(stage_data[stage_input]["file_list"]) > 1:
                # Square bracketed csv list
                file_id_list = [
                    file_id
                    for file_id in stage_data[stage_input]["file_list"]
                ]
                file_ids = "[{file_ids}]".format(file_ids=",".join(file_id_list))
                values.append("")  # No need to provide file name in batch file
                values.append(file_ids)

        # add all headers for every sample (for sense check later)
        batch_headers.append(tuple(headers))

        assert len(set(map(len, batch_headers))) == 1, (
            "Sample {} doesn't have the same number of files gathered. "
            "Check if no single jobs failed/fastqs "
            "for this sample were given".format(type_input)
        )

        # add values for every sample
        batch_values.append(values)

    return (batch_headers, batch_values)


def create_batch_file(headers, values):
    """ Create batch file + return filename

    Args:
        headers (tuple): Tuple of headers
        values (list): List of the values that need to be written for every line

    Returns:
        str: Batch filename
    """

    batch_uuid = str(uuid.uuid4())
    batch_filename = ".".join([batch_uuid, "tsv"])

    # check if all headers gathered are identical
    assert len(set(headers)) == 1, (
        "All the headers retrieved are not identical\n"
    )

    uniq_headers = headers[0]

    # Write the file content
    with open(batch_filename, "w") as b_fh:
        tsv_line = "\t".join(uniq_headers) + "\n"
        b_fh.write(tsv_line)

        for line in values:
            tsv_line = "\t".join(line) + "\n"
            b_fh.write(tsv_line)

    return batch_filename


def assess_batch_file(batch_file):
    """ Check if the batch file has the same number of headers and values

    Args:
        batch_file (str): Batch file path

    Returns:
        int: Line number where the number of values is not the same as the number of headers
    """

    with open(batch_file) as f:
        # for every line check if the number of values is equal
        # to the number of headers
        for index, line in enumerate(f):
            if index == 0:
                headers = line.strip().split("\t")
            else:
                values = line.strip().split("\t")

                if len(headers) != len(values):
                    return index + 1

    return True


def find_previous_coverage_reports(sample):
    """ Return the coverage reports for given sample if they exist

    Args:
        sample (str): Sample id

    Returns:
        list: List of coverage reports
    """

    # go find coverage reports that have the same sample id
    cmd = "dx find data --path / --name {}*coverage_report.html --brief".format(sample)
    output = subprocess.check_output(cmd, shell=True).strip()

    if output == "":
        return None
    else:
        return output.split("\n")


def get_next_index(file_ids):
    """ Return the index to assign to the new coverage report

    Args:
        file_ids (list): List of coverage reports

    Returns:
        int: Index to assign to the new coverage report
    """

    index_to_return = 1

    indexes = []

    # other reports found
    if file_ids:
        for file_id in file_ids:
            name = get_object_attribute_from_object_id_or_path(file_id, "Name")
            index = name.split("_")[1]

            # check that the element is indeed a number
            if index.isdigit():
                # add it to the list of indices
                indexes.append(index)

        assert indexes != [], (
            "Couldn't find file names for"
            "{}".format(file_ids)
        )

        # found some reports return the highest number + 1
        return int(max(indexes)) + 1

    # no reports found, just return 1
    else:
        return index_to_return


def get_latest_config(folder):
    """ Get the latest config given a folder containing the version folders

    Args:
        folder (str): Folder containing folders for every version of the config

    Returns:
        str: Latest version of the config
    """

    # loop through folders in the folder config given and parse the folder name
    # using the packaging package and get the max value
    config_latest_version = str(max(
        [version.parse(str(f)) for f in os.listdir(folder)]
    ))
    return config_latest_version


def parse_manifest(manifest_file_id):
    """ Parse manifest

    Args:
        manifest_file_id (str): DNAnexus file id for manifest file

    Returns:
        dict: Dict of samples linked to panels and clinical indications
    """

    data = defaultdict(lambda: defaultdict(set))

    project_id, manifest_id = manifest_file_id.split(":")

    with dxpy.open_dxfile(manifest_id, project=project_id) as f:
        for line in f:
            sample, clinical_indication, panel, gene = line.strip().split("\t")
            data[sample]["clinical_indications"].add(clinical_indication)
            data[sample]["panels"].add(panel)

    return data


def gather_sample_sheet():
    """ Get sample sheet id

    Returns:
        str: Sample sheet DNAnexus id
    """

    # get the project id from the environment variable
    current_project = os.environ.get('DX_PROJECT_CONTEXT_ID')
    result = dxpy.find_data_objects(
        classname="file", name="SampleSheet.csv", project=current_project
    )

    sample_sheets = []

    for sample_sheet in result:
        sample_sheets.append(sample_sheet)

    # quick check to see if we get none or multiple
    assert len(sample_sheets) == 1, "Didn't gather only one sample sheet file"

    return sample_sheets[0]["id"]
