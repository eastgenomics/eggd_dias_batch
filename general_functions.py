#!/usr/bin/python

from collections import defaultdict
import datetime
import os
import subprocess
import uuid
import dxpy

from packaging import version

# Generic functions


def get_datetime():
    """ Get the date in YYMMDD format

    Returns:
        str: String of date in YYMMDD format
    """

    date = datetime.datetime.now().strftime("%y%m%d")
    time = datetime.datetime.now().strftime("%H%M")
    return date, time


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


def get_workflow_stage_info(workflow_id): # reports
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


def make_app_out_dirs(workflow_stage_info, workflow_output_dir): # reports
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


def format_relative_paths(workflow_stage_info): # reports
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


def find_app_dir(workflow_output_dir, app_basename): # reports
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


def get_stage_input_file_list(app_dir, app_subdir="", filename_pattern="."): # reports
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


def dx_get_project_id():
    """ Return project id using dx env

    Returns:
        str: DNAnexus project id
    """

    project_id = os.environ.get('DX_PROJECT_CONTEXT_ID')

    return project_id


def dx_get_object_name(object_id):
    """ Get specific attribute from DNAnexus description of object

    Args:
        object_id (str): DNAnexus object ID

    Returns:
        str: Name of DNAnexus object
    """

    # This try except is used to handle permission errors generated when
    # dx describe tries to get info about files we do not have permission
    # to access.
    # In these cases the description is returned but the command has non-0
    # exit status so errors out
    try:
        object_name = dxpy.describe(object_id)['name']
        return object_name
    except dxpy.exceptions.DXError:
        print("Object ID was not provided in the correct format")
        return None


def parse_samplesheet(sample_sheet_ID): # reports
    """ Return list of samples from the sample sheet

    Args:
        sample_sheet_ID (str): DNAnexus file-ID of the SampleSheet

    Returns:
        list: List of sample names
    """

    sample_names = []
    cmd = "dx cat {}".format(sample_sheet_ID)
    # Stream content of the SampleSheet from DNAnexus
    sample_sheet_content = subprocess.check_output(cmd, shell=True)
    # Split content into non-empty lines
    lines = sample_sheet_content.split("\n")[:-1]

    data = False

    # Loop over lines and parse sample ID/names
    for line in lines:
        if data is False:
            # skip lines until reaching [Data] section with "Sample_ID"
            if line.startswith("Sample_ID"):
                data = True
        else:
            # SampleSheet always has a comma separated [Data] section
            Sample_ID = line.split(",")[0]
            sample_names.append(Sample_ID)

    return sample_names


def make_workflow_out_dir(workflow_id, assay_id, workflow_out_dir="/output/"): # reports
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
    date, time = get_datetime()

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


def get_stage_inputs(input_dir, sample_name_list, stage_input_pattern_dict): # reports
    """ Return dict with sample2stage2files

    Args:
        input_dir (str): Directory of single workflow
        sample_name_list (list): list of sample names
        stage_input_pattern_dict (dict): Dict of stage input search patterns

    Returns:
        dict: Dict of sample2stage2file_list
    """

    # Allows me to not have to check if a key exists before creating an entry in the dict
    # Example: dict[entry][sub-entry][list-entry].append(ele)
    sample2stage_input2files_dict = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list))
    )

    for sample in sample_name_list:
        # make a placeholder for the sample
        sample2stage_input2files_dict[sample] = {}
        # find the inputs for each stage using given app/pattern
        for stage_input, stage_input_info in stage_input_pattern_dict.items():
            if len(input_dir.split("/")) < 3:
                input_app_dir = find_app_dir(
                    input_dir, stage_input_info["app"]
                )
            else:
                input_app_dir = input_dir

            inputs = get_stage_input_file_list(
                input_app_dir,
                app_subdir=stage_input_info["subdir"],
                filename_pattern=stage_input_info["pattern"].format(sample)
            )
            sample2stage_input2files_dict[sample][stage_input] = inputs

    return sample2stage_input2files_dict


def prepare_batch_writing(
    stage_input_dict, type_workflow, assay_config_happy_stage_prefix=None,
    assay_config_somalier_relate_stage_id=None,assay_config_athena_stage_id=None,
    assay_config_generate_workbook_stage_id=None, workflow_specificity={}
): # reports
    """ Return headers and values for the batch file writing

    Args:
        stage_input_dict (dict): Dict of sample2stage2file_list
        type_workflow (str): String equal to either multi or reports
        assay_config_happy_stage_prefix (str): Hap.py stage id from the assay config
        assay_config_somalier_relate_stage_id (str): Somalier relate stage id from the assay config
        assay_config_athena_stage_id (str): Athena stage id from the assay config
        assay_config_generate_workbook_stage_id (str): workbooks stage id from the assay config
        workflow_specificity (dict, optional): For the reports, add the dynamic files to headers + values. Defaults to {}.

    Returns:
        tuple: Tuple of headers and values
    """
    batch_headers = []
    batch_values = []


    for sample in stage_input_dict:
        stage_data = stage_input_dict[sample]
        headers = ["batch ID"]
        values = []

        if type_workflow == "reports" or type_workflow == "cnvreports":
            values.append(sample)

            # get the index for the coverage report that needs to be created
            coverage_reports = find_previous_reports(
                sample, "coverage_report.html"
            )
            coverage_index = get_next_index(coverage_reports)
            # get the index for the xls report that needs to be created
            xls_reports = find_previous_reports(sample, ".xls*")
            xls_index = get_next_index(xls_reports)

            index_to_use = max([xls_index, coverage_index])

            # CNV reports currently does not have athena so we skip adding
            # athena headers if its cnvreports
            if type_workflow != "cnvreports":
                # add the name param to athena
                headers.append("{}.name".format(assay_config_athena_stage_id))
                # add the value of name to athena
                values.append("{}_{}".format(sample, index_to_use))

            # add the name output_prefix to generate_workbooks
            headers.append("{}.output_prefix".format(
                    assay_config_generate_workbook_stage_id
                )
            )

            # add the value of output prefix to generate workbooks
            values.append("{}_{}".format(sample, index_to_use))

        # add the dynamic files to the headers and values
        for stage, file_id in workflow_specificity.items():
            headers.append(stage)  # col for file name
            values.append(file_id)

        # For each stage add the column header and the values in that column
        for stage_input in stage_data:
            if len(stage_data[stage_input]) == 0:
                continue

            headers.append(stage_input)  # col for file name
            headers.append(" ".join([stage_input, "ID"]))  # col for file ID

            # One file in file list - no need to merge into array
            if len(stage_data[stage_input]) == 1:
                if "{}".format(assay_config_somalier_relate_stage_id) in stage_input:
                    file_ids = stage_data[stage_input]
                else:
                    file_ids = stage_data[stage_input][0]

                values.append("")  # No need to provide file name in batch file
                values.append(file_ids)

            # make a square bracketed comma separated list if multiple input files
            elif len(stage_data[stage_input]) > 1:
                # Square bracketed csv list
                file_id_list = [
                    file_id
                    for file_id in stage_data[stage_input]
                ]
                file_ids = "[{file_ids}]".format(file_ids=",".join(file_id_list))
                values.append("")  # No need to provide file name in batch file
                values.append(file_ids)

        # add all headers for every sample (for sense check later)
        batch_headers.append(tuple(headers))

        assert len(set(map(len, batch_headers))) == 1, (
            "Sample {} doesn't have the same number of files gathered. "
            "Check if no single jobs failed/fastqs "
            "for this sample were given".format(sample)
        )

        # add values for every sample
        batch_values.append(values)

    return (batch_headers, batch_values)


def create_batch_file(headers, values): # reports
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


def assess_batch_file(batch_file): # reports
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


def find_previous_reports(sample, suffix): # reports
    """ Return the reports for given sample if they exist

    Args:
        sample (str): Sample id
        suffix (str): Suffix of the report to find

    Returns:
        list: List of coverage reports
    """

    current_project = os.environ.get('DX_PROJECT_CONTEXT_ID')
    report_name = "{}*{}".format(sample, suffix)

    output = dxpy.find_data_objects(
        name=report_name, project=current_project, name_mode="glob"
    )

    reports = []

    for dnanexus_dict in output:
        report = dxpy.DXFile(dnanexus_dict["id"])
        reports.append(report.name.split(".")[0])

    return reports


def get_next_index(file_names): # reports
    """ Return the index to assign to the new report

    Args:
        file_names (list): List of reports

    Returns:
        int: Index to assign to the new report
    """

    index_to_return = 1

    indexes = []

    # other reports found
    if file_names:
        for file_name in file_names:
            index = file_name.split("_")[1]

            # check that the element is indeed a number
            if index.isdigit():
                # add it to the list of indices
                indexes.append(index)

        assert indexes != [], (
            "Couldn't find file names for {}".format(file_names)
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


def parse_Epic_manifest(project_id, manifest_file_id): # reports
    """ Parse manifest from Epic

    Args:
        manifest_file_id (str): DNAnexus file id for manifest file
            should contain one sample per row, with
            multiple clinical indications separated by semicolon

    Returns:
        dict: Dict of samples linked to list of clinical indications
            partial sample identifiers and R_code or _HGNC ID
    """
    data = {}
    if manifest_file_id.startswith("project"):
        project_ID, manifest_id = manifest_file_id.split(":")[1]
        if project_id != project_ID:
            print("WARNING! Manifest file is provided from a different project!")
    else:
        manifest_id = manifest_file_id

    with dxpy.open_dxfile(manifest_id, project=project_id, mode='r') as f:
        for line in f: # can't skip header, but will be filtered out later
            record = line.strip().split(",") # assuming comma-separated values
            Specimen_ID = record[0].strip("SP-")
            Instrument_ID = record[1]

            sample_identifier = "-".join([Instrument_ID, Specimen_ID])
            clinical_indications = record[-1].split(";") # assuming semicolon-separated values
            R_codes = list(set(
                [CI for CI in clinical_indications if CI.startswith("R") or CI.startswith("_")]
            ))
            # if sample is already assigned to a list of CIs, extend the list
            if sample_identifier in data.keys():
                data[sample_identifier]["CIs"].append(R_codes)
            # if sample has no CIs yet, save the list
            else:
                data[sample_identifier] = {"CIs": R_codes}

    return data


def parse_Gemini_manifest(manifest_file): # reports
    """ Parse manifest from Gemini

    Args:
        manifest_file (str): filename from command arg for manifest file
            should contain one sample per row, with
            multiple clinical indications separated by tab

    Returns:
        dict: Dict of samples linked to list of clinical indications
            partial sample identifiers (X number) and 
            full clinical indication starting with R_code or _HGNC ID
    """
    data = {}

    with open(manifest_file) as f:
        for line in f:
            record = line.strip().split("\t")
            assert len(record) == 2, (
                "Unexpected number of fields in reanalysis_list. "
                "File must contain one tab separated "
                "sample/panel combination per line"
            )
            sample_identifier = record[0] # X number
            clinical_indications = record[1].split(";")
            R_codes = list(set(
                [CI for CI in clinical_indications if CI.startswith("R") or CI.startswith("_")]
            ))
            # if sample is already assigned to a list of CIs, extend the list
            if sample_identifier in data.keys():
                data[sample_identifier]["CIs"].append(R_codes)
            # if sample has no CIs yet, save the list
            else:
                data[sample_identifier] = {"CIs": R_codes}

    return data


def parse_genepanels(genepanels_file_id): # reports
    """ Parse genepanels

    Args:
        genepanels_file_id (str): DNAnexus file id for genepanels file

    Returns:
        dict: Dict of samples linked to panels and clinical indications
    """

    data = {}

    project_id, genepanels_id = genepanels_file_id.split(":")

    with dxpy.open_dxfile(genepanels_id, project=project_id) as f:
        for line in f:
            clinical_indication, panel, gene = line.strip().split("\t")
            data.setdefault(clinical_indication, set()).add(panel)

    return data


def gather_samplesheet(): # reports
    """ Get file-ID of SampleSheet within given project

    Returns:
        str: DNAnexus file-ID of the Sample sheet
    """

    # get the project id from the environment variable
    current_project = os.environ.get('DX_PROJECT_CONTEXT_ID')
    results = dxpy.find_data_objects(
        classname="file", name="SampleSheet.csv", project=current_project
    )

    sample_sheets = []

    for sample_sheet in results:
        sample_sheets.append(sample_sheet)

    # quick check to see if we get none or multiple
    assert len(sample_sheets) == 1, "Didn't gather only one sample sheet file"

    return sample_sheets[0]["id"]

def find_files(project_name, app_dir, pattern="."): # reports
    """Searches for files ending in provided pattern (e.g "*bam") in a
    given path that contains the files that are being searched for
    (e.g /output/single/sentieon_output).

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
                pattern, app_dir
            ))

    return search_result

def make_app_output_dir(app_id, ss_workflow_out_dir, app_name, assay_id):
    """Creates directory for single app with version, date and attempt

    Args:
        app_id (str): CNV app ID
        ss_workflow_out_dir (str): single workflow string
        app_name (str): App name
        assay_id (str): assay ID with the version

    Returns:
        None: folder created in function dx_make_workflow_dir
    """
    # remove trailing forward dash in ss_workflow
    ss_workflow_out_dir = ss_workflow_out_dir.rstrip('/')
    app_version = str(dxpy.describe(app_id)['version'])

    app_output_dir_pattern = "{ss_workflow_out_dir}/{app_name}_v{version}-{assay}-{date}-{index}/"
    date, time = get_datetime()

    # when creating the new folder, check if the folder already exists
    # increment index until it works or reaches 100
    i = 1
    while i < 100:  # < 100 runs = sanity check
        app_output_dir = app_output_dir_pattern.format(
            ss_workflow_out_dir=ss_workflow_out_dir,
            app_name=app_name,version=app_version,
            assay=assay_id, date=date, index=i
        )

        if dx_make_workflow_dir(app_output_dir):
            print("Using\t\t%s" % app_output_dir)
            return app_output_dir
        else:
            print("Skipping\t%s" % app_output_dir)

        i += 1

    return None


def create_job_reports(rpt_out_dir, all_samples, job_dict):
    """ Create and upload a job report file where reports are categorised in:
        - expected samples
        - running jobs for samples found
        - missing samples from the manifest
        - samples with gene symbols as panels causing them to fail later on

    Args:
        rpt_out_dir (str): Dias reports directory
        all_samples (list): List with all samples in sample sheet minus NA
        job_dict (dict): Dict with the lists of samples for the categories
        listed at the top of the docstring

    Returns:
        str: Name and path of job report file
    """

    # rpt_out_dir should always be /output/dias_single/dias_reports but in case
    # someone adds a "/" at the end, which I do sometimes
    date, time = get_datetime()
    job_report = "".join(["report", date, time, ".txt"])

    # get samples for which a cnvreport is expected but the job will not start
    # for reasons other than absence from manifest
    # eg. present in SampleSheet but CNV calling output files are not available
    difference_expected_starting = set(all_samples).difference(
        set(job_dict["starting"])
    )

    with open(job_report, "w") as f:
        f.write(
            "Number of reports expected: {}\n\n".format(len(all_samples))
        )

        f.write(
            "Number of samples for which a job started: {}\n".format(
                len(job_dict["starting"])
            )
        )

        f.write("Samples for which jobs didn't start:\n")

        if difference_expected_starting:
            for sample_id in difference_expected_starting:
                f.write("{}\n".format(sample_id))

        f.write(
            "\nSamples not found in manifest: {}\n".format(
                len(job_dict["missing_from_manifest"])
            )
        )

        if job_dict["missing_from_manifest"]:
            for sample_id in job_dict["missing_from_manifest"]:
                f.write("{}\n".format(sample_id))

        f.write(
            "\nSamples booked with gene symbols: {}\n".format(
                len(job_dict["symbols"])
            )
        )

        if job_dict["symbols"]:
            for sample_id, panels in job_dict["symbols"]:
                f.write("{}\t{}\n".format(sample_id, panels))

    cmd = "dx upload {} --path {}".format(job_report, rpt_out_dir)
    subprocess.check_output(cmd, shell=True)

    return "{}{}".format(rpt_out_dir, job_report)
