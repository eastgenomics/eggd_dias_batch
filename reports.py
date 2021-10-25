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
    assess_batch_file
)

# reanalysis


def gather_sample_ids_from_bams(ss_workflow_out_dir):
    """ Get a list of sample ids from the bams files

    Args:
        ss_workflow_out_dir (str): Path of single folder workflow in DNAnexus

    Returns:
        list: List of samples ids
    """

    cmd = "dx ls {}/sentieon-dnaseq/*bam".format(ss_workflow_out_dir)
    bams = subprocess.check_output(cmd, shell=True).strip().split("\n")
    # get the sample name from the bam and take the X number out of the sample name
    sample_list = [bam.split("_")[0].split("-")[0] for bam in bams]
    return sample_list


def run_reanalysis(input_dir, dry_run, assay_config, assay_id, reanalysis_list):
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

    run_reports(
        input_dir, dry_run, assay_config, assay_id,
        reanalysis_dict=reanalysis_dict
    )


def run_reports(
    ss_workflow_out_dir, dry_run, assay_config, assay_id, reanalysis_dict=None
):
    assert ss_workflow_out_dir.startswith("/"), (
        "Input directory must be full path (starting at /)")
    rpt_workflow_out_dir = make_workflow_out_dir(
        assay_config.rpt_workflow_id, assay_id, ss_workflow_out_dir
    )

    rpt_workflow_stage_info = get_workflow_stage_info(
        assay_config.rpt_workflow_id
    )
    rpt_output_dirs = make_app_out_dirs(
        rpt_workflow_stage_info, rpt_workflow_out_dir
    )

    sample2stage_input_dict = {}

    if reanalysis_dict:
        stage_input_dict = assay_config.rea_stage_input_dict
        sample_id_list = reanalysis_dict
    else:
        stage_input_dict = assay_config.rpt_stage_input_dict
        sample_id_list = gather_sample_ids_from_bams(ss_workflow_out_dir)

    # put the sample id in a dictionary so that the stage inputs can be
    # assigned to a sample id
    for sample in sample_id_list:
        sample2stage_input_dict[sample] = stage_input_dict

    # get the inputs for the given app-pattern
    staging_dict = get_stage_inputs(
        ss_workflow_out_dir, sample2stage_input_dict
    )

    if reanalysis_dict:
        # reanalysis requires list of panels for vcf2xls
        # reanalysis requires panel name for generate_bed
        headers = []
        values = []

        # get the headers and values from the staging inputs
        rea_headers, rea_values = prepare_batch_writing(
            staging_dict, "reports", assay_config, assay_config.rea_dynamic_files
        )

        # manually add the headers for reanalysis vcf2xls/generate_bed
        # rea_headers contains the headers for the batch file
        for header in rea_headers:
            new_headers = [field for field in header]
            new_headers.append(
                "{}.list_panel_names_genes".format(assay_config.vcf2xls_stage_id)
            )
            new_headers.append("{}.panel".format(assay_config.generate_bed_stage_id))
            new_headers.append("{}.panel".format(assay_config.generate_bed_xlsx_stage_id))
            headers.append(tuple(new_headers))

        # manually add the values for reanalysis vcf2xls/generate_bed
        # rea_values contains the values for the headers for the batch file
        for line in rea_values:
            # get all panels in a string and store it in a list with one ele
            panels = [
                ";".join(panel) for sample, panel in reanalysis_dict.items()
                if line[0] == sample
            ]
            # add panels three times for vcf2xls, generate_bed, generate_bed_vcf2xls
            line.extend(panels)
            line.extend(panels)
            line.extend(panels)
            values.append(line)
    else:
        # get the headers and values from the staging inputs
        headers, values = prepare_batch_writing(
            staging_dict, "reports", assay_config, assay_config.rpt_dynamic_files
        )

    rpt_batch_file = create_batch_file(headers, values)

    command = "dx run -y --rerun-stage '*' {} -istage-G4BJkJQ4JxJvBv5vJq50vJZ8.flank={} --batch-tsv={}".format(
        assay_config.rpt_workflow_id, assay_config.xlsx_flanks , rpt_batch_file
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
