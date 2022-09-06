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
    get_sample_ids_from_sample_sheet,
    gather_sample_sheet
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
    # someone adds a "/" at the end, which i do sometimes
    name_file = [
        ele for ele in rpt_out_dir.split('/') if ele.startswith("dias_cnvreports")
    ]
    # there should only be one ele in name_file
    if len(name_file) != 1:
        raise Exception("reports output directory '{}' contains two "
                        "dias_cnvreports".format(rpt_out_dir))
    job_report = "{}.txt".format(name_file[0])

    # get samples for which a report is expected but the job will not started
    # for reasons other than absence from manifest
    # i.e. present in sample sheet but fastqs were not provided
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


# reanalysis


def run_cnvreanalysis(input_dir, dry_run, assay_config, assay_id, reanalysis_list):
    """Reads in the reanalysis file given on the command line and runs the
    CNV reports script.

    Args:
        input_dir: single output directory e.g /output/dias_single
        dry_run: optional arg command from cmd line if its a dry run
        assay_config: contains all the dynamic DNAnexus IDs
        assay_id: optional arg command from cmd line for what assay this is
        reanalysis_list: reanalysis file provided on the cmd line
    """


    reanalysis_dict = {}

    # parse reanalysis file
    with open(reanalysis_list) as r_fh:
        for line in r_fh:
            fields = line.strip().split("\t")
            assert len(fields) == 2, (
                "Unexpected number of fields in reanalysis_list. "
                "File must contain one tab separated "
                "sample/panel combination per line"
            )
            sample, panel = fields
            panels = panel.split(";")

            for panel in panels:
                # get a dict of sample2panels
                reanalysis_dict.setdefault(sample, set()).add(panel)

    run_cnvreports(
        input_dir, dry_run, assay_config, assay_id,
        reanalysis_dict=reanalysis_dict
    )


def run_cnvreports(
    ss_workflow_out_dir, dry_run, assay_config, assay_id, reanalysis_dict=None
):
    """Generates batch script with headers from the reports workflow and
    values from the reports directory and then runs the command.

    Args:
        ss_workflow_out_dir: single output directory e.g /output/dias_single
        dry_run: optional arg command from cmd line if its a dry run
        assay_config: contains all the dynamic DNAnexus IDs
        assay_id: optional arg command from cmd line for what assay this is
        reanalysis_dict: reanalysis file IF provided on the cmd line
    """
    assert ss_workflow_out_dir.startswith("/"), (
        "Input directory must be full path (starting at /)")
    rpt_workflow_out_dir = make_workflow_out_dir(
        assay_config.cnv_rpt_workflow_id, assay_id, ss_workflow_out_dir
    )

    rpt_workflow_stage_info = get_workflow_stage_info(
        assay_config.cnv_rpt_workflow_id
    )
    rpt_output_dirs = make_app_out_dirs(
        rpt_workflow_stage_info, rpt_workflow_out_dir
    )

    sample2stage_input_dict = {}

    if reanalysis_dict:
        stage_input_dict = assay_config.cnv_rea_stage_input_dict
        sample_id_list = reanalysis_dict
    else:
        sample_sheet_path = gather_sample_sheet()
        all_samples = get_sample_ids_from_sample_sheet(sample_sheet_path)
        stage_input_dict = assay_config.cnv_rpt_stage_input_dict
        sample_id_list = all_samples

    # put the sample id in a dictionary so that the stage inputs can be
    # assigned to a sample id
    for sample in sample_id_list:
        sample2stage_input_dict[sample] = stage_input_dict

    # get the inputs for the given app-pattern
    staging_dict = get_stage_inputs(
        ss_workflow_out_dir, sample2stage_input_dict
    )

    # list that is going to represent the header in the batch tsv file
    headers = []
    # list that is going to represent the lines for each sample in the batch
    # tsv file
    values = []

    if reanalysis_dict:
        genepanels_data = parse_genepanels(assay_config.genepanels_file)

        # get the headers and values from the staging inputs
        rea_headers, rea_values = prepare_batch_writing(
            staging_dict, "cnvreports", assay_config.happy_stage_prefix,
            assay_config.somalier_relate_stage_id,
            "",
            assay_config.cnv_generate_workbook_stage_id,
            assay_config.cnv_rea_dynamic_files
        )

        # manually add the headers for reanalysis vcf2xls/generate_bed
        # rea_headers contains the headers for the batch file
        for header in rea_headers:
            new_headers = [field for field in header]
            new_headers.append(
                "{}.clinical_indication".format(
                    assay_config.cnv_generate_workbook_stage_id
                )
            )
            new_headers.append(
                "{}.panel".format(
                    assay_config.cnv_generate_workbook_stage_id
                )
            )
            new_headers.append(
                "{}.panel".format(assay_config.cnv_generate_bed_vep_stage_id)
            )
            new_headers.append(
                "{}.panel".format(assay_config.cnv_generate_bed_excluded_stage_id)
            )
            headers.append(tuple(new_headers))

        # manually add the values for reanalysis workbook/generate_bed
        # rea_values contains the values for the headers for the batch file
        for line in rea_values:
            # get all clinical_indications in a string and store it in a list
            # with one ele
            clinical_indications = [
                ";".join(panel) for sample, panel in reanalysis_dict.items()
                if line[0] == sample
            ]
            # clinical indications for generate_workbook
            line.extend(clinical_indications)

            # gather panels from clinical indications for displaying in
            # generate_workbook
            for sample, cis_in_reanalysis_file in reanalysis_dict.items():
                if line[0] == sample:
                    display_panel_list = []

                    # gather every panel associated with the clinical
                    # indication. Also gather HGNC ids specified in the
                    # reanalysis file
                    for ci in cis_in_reanalysis_file:
                        if not ci.startswith("_"):
                            display_panel_list.append(
                                ";".join(genepanels_data[ci])
                            )
                        else:
                            display_panel_list.append(ci)

            line.append(";".join(display_panel_list))

            # add clinical_indications for generate_bed_vep and
            # generate_bed_athena
            line.extend(clinical_indications)
            line.extend(clinical_indications)
            values.append(line)
    else:
        job_dict = {"starting": [], "missing_from_manifest": [], "symbols": []}

        manifest_data = parse_manifest(assay_config.bioinformatic_manifest)

        # get the headers and values from the staging inputs
        rpt_headers, rpt_values = prepare_batch_writing(
            staging_dict, "cnvreports", assay_config.happy_stage_prefix,
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
            rpt_workflow_out_dir, all_samples, job_dict
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

    command = " ".join([command, app_relative_paths, destination])

    if dry_run:
        print("Created workflow dir: {}".format(rpt_workflow_out_dir))
        print("Stage info:")
        print(json.dumps(
            OrderedDict(sorted(rpt_workflow_stage_info.iteritems())), indent=2)
        )
        print("Inputs gathered:")
        print(json.dumps(staging_dict, indent=4))
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

        print("Format of stage output dirs: {}".format(app_relative_paths))
        print("Final cmd ran: {}".format(command))
        print("Deleting '{}' as part of the dry-run".format(rpt_workflow_out_dir))
        delete_folders_cmd = "dx rm -r {}".format(rpt_workflow_out_dir)
        subprocess.call(delete_folders_cmd, shell=True)
    else:
        subprocess.call(command, shell=True)

    return rpt_workflow_out_dir
