#!/usr/bin/python

from collections import OrderedDict
import json
import subprocess

from general_functions import (
    dx_get_project_id,
    dx_get_object_name,
    get_workflow_stage_info,
    get_stage_inputs,
    make_workflow_out_dir,
    make_app_out_dirs,
    find_app_dir,
    find_files,
    parse_genepanels,
    parse_Epic_manifest,
    parse_Gemini_manifest,
    prepare_batch_writing,
    create_batch_file,
    assess_batch_file,
    format_relative_paths,
    create_job_report_file
)


# reports
def run_reports(
    ss_workflow_out_dir, dry_run, mosaic, assay_config, assay_id,
    sample_ID_TestCode=None, sample_X_CI=None
):
    """Reads in the manifest file given on the command line and runs the
    SNV reports workflow.

    Collects input files for the reports workflow and sets off jobs

    Args:
        ss_workflow_out_dir: DNAnexus path to single output directory
            e.g /output/dias_single or /output/CEN-YYMMDD-HHMM
        dry_run: optional flag to set up but not run jobs
        assay_config: contains all the dynamic input file DNAnexus IDs
        assay_id: arg from cmd line what assay this is for
        sample_ID_TestCode: filename of Epic manifest containing sample identifiers
            and specifying C/R codes (or _HGNC IDs) to analyse samples with
            used with reports command
        sample_X_CI: filename of Gemini manifest containing X numbers
            and specifying clinical indications to analyse samples with
            used with reanalysis command
    """

    ### Set up environment: make output folders
    # Find project to create jobs and outdirs in
    project_id = dx_get_project_id()
    project_name = dx_get_object_name(project_id)
    print("Jobs will be set off in project {}".format(project_name))

    # Check that provided input directory is an absolute path
    assert ss_workflow_out_dir.startswith("/output/"), (
        "Input directory must be full path (starting with /output/)")

    # Create workflow output folder named after workflow and config used
    rpt_workflow_out_dir = make_workflow_out_dir(
        assay_config.rpt_workflow_id, assay_id, ss_workflow_out_dir
    )
    # Identify executables for each stage within the workflow
    # stages[stage['id']] = {"app_id": app_id, "app_name": app_name}
    rpt_workflow_stage_info = get_workflow_stage_info(
        assay_config.rpt_workflow_id
    )
    # Create output folders for each executable within the workflow
    rpt_output_dirs = make_app_out_dirs(
        rpt_workflow_stage_info, rpt_workflow_out_dir
    )

    ### Identify samples to run reports workflow for
    # Gather sample names that have a Sentieon VCF generated
    ## current pattern picks up both "normal" and "genomic" VCFs
    sentieon_folder_path = find_app_dir(ss_workflow_out_dir, "sentieon")
    single_sample_vcfs = find_files(
        project_name, sentieon_folder_path, pattern="(.*).vcf.gz$"
    )
    # convert list of VCF file names to set of sample names
    single_sample_names = list(set(
        [str(x).split('_')[0] for x in single_sample_vcfs]
    ))

    ### Identify panels and clinical indications for each sample
    # Placeholder dict for gene_CIs and clinical indications
    # based on test code from manifest (see below)
    sample2CIpanel_dict = {}

    # Placeholder dict for the number of samples with suitable VCF,
    # a list of invalid sample identifiers and
    # a list of sample identifiers with invalid panel/CI requests
    job_report_dict = {
        "total_input": len(single_sample_names),
        "invalid_samples": [], "invalid_tests": []
    }

    # Load genepanels information
    CI2panels_dict = parse_genepanels(assay_config.genepanels_file)

    ## Based on the command arg input, identify samples and panels from the
    ## Epic or Gemini-style manifest file
    if sample_ID_TestCode is not None:
        print("running dias_reports with sample identifiers and test codes "
                "from Epic")
        # Gather samples from the Epic manifest file (command line input file-ID)
        ## manifest_data is a {sample: {test_codes: [], analysis: string}} dict
        # manifest file only has partial sample names/identifiers
        manifest_data = parse_Epic_manifest(sample_ID_TestCode)
        manifest_samples = manifest_data.keys()
        # populate job report dict 
        job_report_dict["total_manifest"] = len(manifest_samples)
        invalid_samples = [sample_identifier for
                sample_identifier, v in manifest_data.items() if
                v["analysis"] == "insufficient"
        ]
        job_report_dict["invalid_samples"] = invalid_samples

        # match partial identifier from available sample names with
        # those from the manifest
        for sample in single_sample_names:
            Instrument_ID = sample.split('-')[0]
            Specimen_ID = sample.split('-')[1]
            partial_identifier = "-".join([Instrument_ID, Specimen_ID])
            if partial_identifier in manifest_samples:
                    manifest_data[partial_identifier]["sample"] = sample

        # With the relevant samples identified,
        # parse the clinical indications (R code or HGNC) they were booked for
        sample2testcodes_dict = dict(
            (sample_CI["sample"], sample_CI["test_codes"]) for sample_CI 
                in manifest_data.values() if "sample" in sample_CI.keys()
        )

        # Get gene panels based on test code from manifest
        for sample, test_codes in sample2testcodes_dict.items():
            skip_sample = False
            # placeholders for storing
            # clinical indication (test code: R, C code or _HGNC:ID)
            CIs = []
            # panel name associated with the above clinical indication
            panels = []
            # test code (R or C code) parsed from the clinical indication text or _HGNC:ID
            prefixes = []
            for test_code in test_codes:
                # single gene based on _HGNC ID
                if test_code.startswith("_HGNC"):
                    CIs.append(test_code)
                    panels.append(test_code)
                    prefixes.append(test_code)
                # clinical indication and panel based on test_code
                else:
                    # look up the corresponding clinical indication text and associated panels
                    # from the genepanels.tsv file (parsed into CI2panels_dict variable)
                    clinical_indication = next(
                        (key for key in CI2panels_dict.keys() if key.split("_")[0] == test_code),
                        None)
                    if clinical_indication is None:
                        # skip sample if CI not found
                        skip_sample = True
                        invalid_test_code = test_code
                        print("Clinical indication for test code {} was not"
                            " found in genepanels file for sample {}".format(
                            test_code, sample
                        ))
                    else:
                        # add the clinical indication text corresponding the test code
                        CIs.append(clinical_indication)
                        # add a list of associated panels (to be displayed in the variant workbooks)
                        panels.extend(list(CI2panels_dict[clinical_indication]))
                        # record the test code (for naming the generated bed file)
                        prefixes.append(test_code)
            if skip_sample:
                # skip sample if CI not found for any of its test_codes
                job_report_dict["invalid_tests"].append(
                    (sample, invalid_test_code)
                )
                continue
            sample2CIpanel_dict[sample] = {
                "clinical_indications": CIs,
                "panels": panels,
                "prefixes": prefixes
            }
        # Upload manifest file
        cmd = "dx upload {} --path {}".format(sample_ID_TestCode, rpt_workflow_out_dir)
        subprocess.check_output(cmd, shell=True)

    elif sample_X_CI is not None:
        print("running dias_reports with X numbers and clinical indications "
                "from Gemini")
        # Gather samples from the Gemini manifest file (command line input filename)
        ## manifest_data is a {sample: {CIs: []}} dict
        # parse reanalysis file into 
        manifest_data = parse_Gemini_manifest(sample_X_CI)
        manifest_samples = manifest_data.keys() # list of tuples
        # populate job report dict 
        job_report_dict["total_manifest"] = len(manifest_samples)
        job_report_dict["invalid_samples"] = ["not applicable"]

        # manifest file only has partial sample names/identifiers
        # identify the full sample name based on the X number
        for sample in single_sample_names:
            partial_identifier = sample.split('-')[0] # X number
            if partial_identifier in manifest_samples:
                    manifest_data[partial_identifier]["sample"] = sample

        # With the relevant samples identified, parse the R codes they were booked
        sample2CIs_dict = dict(
            (sample_CI["sample"], sample_CI["test_codes"]) for sample_CI 
                in manifest_data.values() if "sample" in sample_CI.keys()
        )

        # Get gene panels based on clinical indication from manifest
        for sample, clinical_indications in sample2CIs_dict.items():
            skip_sample = False
            # placeholders for storing
            # clinical indication (test code: R, C code or _HGNC:ID)
            CIs = []
            # panel name associated with the above clinical indication
            panels = []
            # test code (R or C code) parsed from the clinical indication text or _HGNC:ID
            prefixes = []
            for CI in clinical_indications:
                # single gene based on _HGNC ID
                if CI.startswith("_HGNC"):
                    CIs.append(CI)
                    panels.append(CI)
                    prefixes.append(CI)
                else:
                    # look up the corresponding clinical indication text and associated panels
                    # from the genepanels.tsv file (parsed into CI2panels_dict variable)
                    clinical_indication = next(
                        (key for key in CI2panels_dict.keys()
                            if key.split("_")[0] == CI.split("_")[0]), None
                        )
                    if clinical_indication is None:
                        # skip sample if CI not found
                        skip_sample = True
                        invalid_test_code = CI
                        print("Clinical indication for test code {} was not"
                            " found in genepanels file for sample {}".format(
                            CI, sample
                        ))
                    else:
                        # add the clinical indication text corresponding the test code
                        CIs.append(clinical_indication)
                        # add a list of associated panels (to be displayed in the variant workbooks)
                        panels.extend(list(CI2panels_dict[clinical_indication]))
                        # record the test code (for naming the generated bed file)
                        prefixes.append(CI.split("_")[0])
            if skip_sample:
                # skip sample if CI not found for any of its CI
                job_report_dict["invalid_tests"].append(
                    (sample, invalid_test_code)
                )
                continue
            sample2CIpanel_dict[sample] = {
                "clinical_indications": CIs,
                "panels": panels,
                "prefixes": prefixes
            }
        # Upload manifest file
        cmd = "dx upload {} --path {}".format(sample_X_CI, rpt_workflow_out_dir)
        subprocess.check_output(cmd, shell=True)

    else:
        assert sample_ID_TestCode or sample_X_CI, "No file was provided with sample & panel information"

    ### Gather sample-specific input file IDs based on the given app-pattern
    if mosaic:
        sample2stage_input2files_dict = get_stage_inputs(
            ss_workflow_out_dir, sample2CIpanel_dict.keys(), assay_config.rpt_mosaic_input_dict
        )
    else:
        sample2stage_input2files_dict = get_stage_inputs(
            ss_workflow_out_dir, sample2CIpanel_dict.keys(), assay_config.rpt_stage_input_dict
        )


    ### Initialise headers and values for a batch.tsv
    # list to represent the header row in the batch.tsv file
    headers = []
    # list to represent the rows/lines for each sample in the batch.tsv file
    values = []
    # get the headers and values from the staging inputs
    rpt_headers, rpt_values = prepare_batch_writing(
        sample2stage_input2files_dict, "reports",
        assay_config_athena_stage_id=assay_config.athena_stage_id,
        assay_config_generate_workbook_stage_id=assay_config.generate_workbook_stage_id,
        workflow_specificity=assay_config.rpt_dynamic_files
    )

    # manually add the headers for panel/clinical_indication inputs
    for header in rpt_headers:
        new_headers = [field for field in header]
        new_headers.extend([
            "{}.clinical_indication".format(assay_config.generate_workbook_stage_id),
            "{}.panel".format(assay_config.generate_bed_vep_stage_id),
            "{}.panel".format(assay_config.generate_bed_athena_stage_id),
            "{}.panel".format(assay_config.generate_workbook_stage_id),
            "{}.output_file_prefix".format(assay_config.generate_bed_vep_stage_id),
            "{}.output_file_prefix".format(assay_config.generate_bed_athena_stage_id)
        ])
        headers.append(tuple(new_headers))

    for line in rpt_values:
        # sample id is the first element of every list generated by
        # the prepare_batch_writing function
        sample = line[0]

        CI_list = sample2CIpanel_dict[sample]["clinical_indications"]
        panel_list = sample2CIpanel_dict[sample]["panels"]
        prefix_list = sample2CIpanel_dict[sample]["prefixes"]

        # join up potential lists of CIs and panels to align
        # the batch file properly
        CIs = ";".join(CI_list)
        panels = ";".join(panel_list)
        test_codes = "&&".join(prefix_list)
        line.extend([CIs, CIs, CIs, panels, test_codes, test_codes])
        values.append(line)

    job_report_dict["successful"] = len(rpt_values)

    ### Create a batch.tsv
    rpt_batch_file = create_batch_file(headers, values)
    # Check batch file is correct every time
    check_batch_file = assess_batch_file(rpt_batch_file)

    if check_batch_file is True:
        print(
            "Format of the file {} is correct".format(rpt_batch_file)
        )
    else:
        print((
            "Format of the file {} is NOT correct: "
            "number of columns in header doesn't match "
            "number of columns in values at line {}".format(
                rpt_batch_file, check_batch_file
            )
        ))

    ### Create a job report file
    job_report_file = create_job_report_file(job_report_dict)

    ### Create the dx run command
    command = "dx run -y --rerun-stage '*' {} --batch-tsv={}".format(
        assay_config.rpt_workflow_id, rpt_batch_file
    )
    # add flank to the VCF filtering bed at the generate_bed stage
    command += " -i{}.flank={} ".format(
        assay_config.generate_bed_vep_stage_id, assay_config.vep_bed_flank
    )
    # increase buffer for TWE assay at eggd_vep stage
    if assay_config.assay_name == "TWE":
        command += " -i{}.buffer_size=1000".format(assay_config.vep_stage_id)
    # assign output folders
    app_relative_paths = format_relative_paths(rpt_workflow_stage_info)
    command += " --destination={} {} ".format(
        rpt_workflow_out_dir, app_relative_paths
    )

    if dry_run:
        print("Created workflow out dir: {}".format(rpt_workflow_out_dir))
        print("Created stage out dirs: ")
        print(json.dumps(
            OrderedDict(sorted(rpt_output_dirs.iteritems())), indent=4)
        )
        print("Inputs gathered:")
        print(json.dumps(sample2stage_input2files_dict, indent=4))
        print("Job report file created as {}".format(job_report_file))
        print("Final cmd: {}".format(command))
        print("Deleting '{}' as part of the dry-run".format(rpt_workflow_out_dir))
        delete_folders_cmd = "dx rm -r {}".format(rpt_workflow_out_dir)
        subprocess.call(delete_folders_cmd, shell=True)
    else:
        # Upload job report file and set off workflows
        cmd = "dx upload {} --path {}".format(job_report_file, rpt_workflow_out_dir)
        subprocess.check_output(cmd, shell=True)
        subprocess.call(command, shell=True)

    return rpt_workflow_out_dir
