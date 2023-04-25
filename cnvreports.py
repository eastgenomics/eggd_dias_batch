#!/usr/bin/python

from collections import OrderedDict
import json
import subprocess

from general_functions import (
    format_relative_paths,
    get_workflow_stage_info,
    make_app_out_dirs,
    make_workflow_out_dir,
    get_stage_inputs,
    prepare_batch_writing,
    create_batch_file,
    assess_batch_file,
    parse_manifest,
    parse_genepanels,
    gather_samplesheet,
    parse_samplesheet,
    find_files,
    dx_get_project_id,
    dx_get_object_name
)


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
    name_file = [
        ele for ele in rpt_out_dir.split('/') if ele.startswith("dias_cnvreports")
    ]
    # there should only be one ele in name_file
    assert len(name_file) == 1, "cnvreports output directory '{}' contains nested dias_cnvreports".format(rpt_out_dir)
    job_report = "{}.txt".format(name_file[0])

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


# cnvreports
def run_cnvreports(
    cnv_calling_out_dir, dry_run, assay_config, assay_id, sample_panel
):
    """Generates batch script with headers from the reports workflow and
    values from the reports directory and then runs the command.

    Args:
        cnv_calling_out_dir: cnvcall output directory e.g /output/dias_single/cnvapp
        dry_run: optional arg from cmd line if its a dry run
        assay_config: contains all the dynamic input file DNAnexus IDs
        assay_id: arg from cmd line what assay this is for
        cnvreanalysis_list: reanalysis file provided on the cmd line
    """

    # Find project to create jobs and outdirs in
    project_id = dx_get_project_id()
    project_name = dx_get_object_name(project_id)
    print("Jobs will be set off in project {}".format(project_name))

    # Check that provided input directory is an absolute path
    assert cnv_calling_out_dir.startswith("/output"), (
        "Input directory must be full path (starting with /output)")

    # split the input directory path up to dias_single outdir path
    ss_workflow_out_dir = cnv_calling_out_dir.rsplit("/",2)[0] + "/"
    # need to ensure that the cnv calling app dir is in the dir name
    if 'GATKgCNV_call' not in cnv_calling_out_dir:
        raise AssertionError("Directory path requires cnv calling app directory")

    # Create workflow output folder named after workflow and config used
    rpt_workflow_out_dir = make_workflow_out_dir(
        assay_config.cnv_rpt_workflow_id, assay_id, ss_workflow_out_dir
    )
    # Identify executables for each stage within the workflow
    # stages[stage['id']] = {"app_id": app_id, "app_name": app_name}
    rpt_workflow_stage_info = get_workflow_stage_info(
        assay_config.cnv_rpt_workflow_id
    )
    # Create output folders for each executable within the workflow
    rpt_output_dirs = make_app_out_dirs(
        rpt_workflow_stage_info, rpt_workflow_out_dir
    )

    # Gather samples from SampleSheet.csv
    sample_sheet_ID = gather_samplesheet()
    samplesheet_sample_names = parse_samplesheet(sample_sheet_ID)

    # Gather sample names that have a segments.VCF generated by CNV calling
    cnvcall_sample_vfcs = find_files(project_name, cnv_calling_out_dir, pattern="-E '(.*)_segments.vcf$'")
    cnvcall_sample_names = [str(x).split('_')[0] for x in cnvcall_sample_vfcs]

    # Gather samples from the manifest file (command line input file-ID)
    ## manifest_data is a {sample: [R, codes]} dict
    manifest_data = parse_manifest(project_id, sample_panel)
    manifest_samples = manifest_data.keys() # list of tuples

    # Collate list of sample names that are available in all 3 lists:
    sample_name_list = []
    # Keep the samplesheet samples that have a VCF
    available_samples = set(cnvcall_sample_names).intersection(samplesheet_sample_names)
    # manifest list only has partial sample names/identifiers
    for sample in available_samples:
        Instrument_ID = sample.split('-')[0]
        Specimen_ID = sample.split('-')[1]
        partial_identifier = "-".join([Instrument_ID, Specimen_ID])
        if partial_identifier in manifest_samples:
                sample_name_list.append(sample)
                manifest_data[partial_identifier]["sample"] = sample

    # Gather sample-specific input file IDs based on the given app-pattern
    sample2stage_input2files_dict = get_stage_inputs(
        ss_workflow_out_dir, sample_name_list, assay_config.cnv_rpt_stage_input_dict
    )

    # list that is going to represent the header in the batch tsv file
    headers = []
    # list that is going to represent the lines for each sample in the batch
    # tsv file
    values = []

    job_dict = {"starting": [], "missing_from_manifest": [], "symbols": []}

    manifest_data = parse_manifest(assay_config.bioinformatic_manifest)

    # get the headers and values from the staging inputs
    rpt_headers, rpt_values = prepare_batch_writing(
        sample2stage_input2files_dict, "cnvreports",
        assay_config.happy_stage_prefix,
        assay_config.somalier_relate_stage_id,
        "",
        assay_config.cnv_generate_workbook_stage_id,
        assay_config.cnv_rpt_dynamic_files
    )

    # manually add the headers for reanalysis vcf2xls/generate_bed
    # rea_headers contains the headers for the batch file
    for header in rpt_headers:
        new_headers = [field for field in header]
        new_headers.append(
            "{}.clinical_indication".format(assay_config.cnv_generate_workbook_stage_id)
        )
        new_headers.append(
            "{}.panel".format(assay_config.cnv_generate_workbook_stage_id)
        )
        headers.append(tuple(new_headers))

    for line in rpt_values:
        # sample id is the first element of every list according to
        # the prepare_batch_writing function
        sample_id = line[0]

        if sample_id in manifest_data:
            cis = manifest_data[sample_id]["clinical_indications"]
            panels = manifest_data[sample_id]["panels"]

            # get single genes with the sample
            single_genes = [
                panel for panel in panels if panel.startswith("_")
            ]

            # if there are single genes
            if single_genes:
                # check if they are HGNC ids
                symbols = [gene.startswith("_HGNC") for gene in single_genes]

                # if they are not, assume it is gene symbols or at least
                # something is going on and needs checking
                if not all(symbols):
                    job_dict["symbols"].append(
                        (sample_id, ";".join(cis))
                    )
                    continue

            job_dict["starting"].append(sample_id)
            # join up potential lists of cis and panels to align the batch
            # file properly
            cis = ";".join(cis)
            panels = ";".join(panels)
            line.append(cis)
            line.append(panels)
            values.append(line)
        else:
            job_dict["missing_from_manifest"].append(sample_id)

    report_file = create_job_reports(
        rpt_workflow_out_dir, sample_name_list, job_dict
    )

    print("Created and uploaded job report file: {}".format(report_file))

    rpt_batch_file = create_batch_file(headers, values)

    args = ""
    args += "-i{}.flank={} ".format(
        assay_config.cnv_generate_bed_vep_stage_id, assay_config.xlsx_flanks
    )

    args += "-i{}.config_file={} ".format(
        assay_config.cnv_vep_stage_id, assay_config.cnv_vep_config
    )

    if assay_config.assay_name == "TWE":
        args += "-i{}.buffer_size=1000".format(assay_config.vep_stage_id)

    command = "dx run -y --rerun-stage '*' {} {} --batch-tsv={}".format(
        assay_config.cnv_rpt_workflow_id, args, rpt_batch_file
    )

    # assign stage out folders
    app_relative_paths = format_relative_paths(rpt_workflow_stage_info)
    destination = " --destination={} ".format(rpt_workflow_out_dir)

    final_command = " ".join([command, app_relative_paths, destination])

    if dry_run:
        print("Created workflow dir: {}".format(rpt_workflow_out_dir))
        print("Stage info:")
        print(json.dumps(
            OrderedDict(sorted(rpt_workflow_stage_info.iteritems())), indent=2)
        )
        print("Inputs gathered:")
        print(json.dumps(sample2stage_input2files_dict, indent=4))
        print("Created apps out dir: {}")
        print(json.dumps(
            OrderedDict(sorted(rpt_output_dirs.iteritems())), indent=4)
        )
        print("Created batch tsv: {}".format(rpt_batch_file))

        check_batch_file = assess_batch_file(rpt_batch_file)

        if check_batch_file is True:
            print(
                "{}: Format of the file is correct".format(rpt_batch_file)
            )
        else:
            print((
                "Number of columns in header doesn't match "
                "nb of columns in values at line {}".format(check_batch_file)
            ))

        print("Stage output dirs: {}".format(app_relative_paths))
        print("Final cmd: {}".format(final_command))
        print("Deleting '{}' as part of the dry-run".format(rpt_workflow_out_dir))
        delete_folders_cmd = "dx rm -r {}".format(rpt_workflow_out_dir)
        subprocess.call(delete_folders_cmd, shell=True)
    else:
        subprocess.call(final_command, shell=True)

    return rpt_workflow_out_dir
