#!/usr/bin/python

import os
import subprocess
import uuid

from config import (
    vcf2xls_applet_id, exons_nirvana,
    g2t, bio_manifest, genepanels,
)
from general_functions import (
    get_object_attribute_from_object_id_or_path,
    dx_make_workflow_dir
)

# vcf2xls


def make_vcf2xls_batch_file(input_directory):
    # uuids for temp files to prevent collisions during parallel runs

    # Input dir is multi sample output dir. We want input files from the parent single sample dir
    assert input_directory.startswith("/"), (
        "Input directory must be full path (starting at /)")
    command = "dx cd {}; dx cd ..".format(input_directory)
    subprocess.check_call(command, shell=True)
    batch_uuid = str(uuid.uuid4())
    batch_tsv = "{}.0000.tsv".format(batch_uuid)

    sample = "(.*)"

    command = (
        "dx generate_batch_inputs "
        "-iannotated_vcf='{sample}_markdup(.*)annotated.vcf$' "
        "-iraw_vcf='{sample}_markdup(.*)Haplotyper.vcf.gz$$' "
        "-isample_coverage_file='{sample}_markdup(.*)nirvana_20(.*)_5bp.gz$' "
        "-isample_coverage_index="
        "'{sample}_markdup(.*)nirvana_20(.*)_5bp.gz.tbi$' "
        "-iflagstat_file='{sample}_markdup.flagstat' "
        "-o {batch_uuid}"
    ).format(sample=sample, batch_uuid=batch_uuid)

    FNULL = open(os.devnull, 'w')
    subprocess.call(
        command, stderr=subprocess.STDOUT, stdout=FNULL, shell=True
    )
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
    # Dynamic - run dependent
    command = (
        "dx find data --path {}expected_depth_v1.1.2/ --name *gz --brief"
    ).format(ms_workflow_out_dir)
    runfolder_coverage_file = subprocess.check_output(
        command, shell=True
    ).strip()

    command = (
        "dx find data --path {}expected_depth_v1.1.2/ --name *gz.tbi --brief"
    ).format(ms_workflow_out_dir)
    runfolder_coverage_index = subprocess.check_output(
        command, shell=True
    ).strip()

    vcf2xls_applet_name = get_object_attribute_from_object_id_or_path(
        vcf2xls_applet_id, "Name"
    )
    vcf2xls_applet_out_dir = "".join(
        [ms_workflow_out_dir, vcf2xls_applet_name]
    )

    batch_file = make_vcf2xls_batch_file(ms_workflow_out_dir)
    if reanalysis_dict:
        batch_file = make_reanalysis_batch_file(batch_file, reanalysis_dict)
    dx_make_workflow_dir(vcf2xls_applet_out_dir)

    command = (
        "dx run {} --yes --destination='{}' "
        "--batch-tsv='{}' -irunfolder_coverage_file='{}' "
        "-irunfolder_coverage_index='{}' -igenepanels_file='{}' "
        "-ibioinformatic_manifest='{}' -iexons_nirvana='{}' "
        "-inirvana_genes2transcripts='{}' "
    ).format(
        vcf2xls_applet_id, vcf2xls_applet_out_dir,
        batch_file, runfolder_coverage_file,
        runfolder_coverage_index, genepanels,
        bio_manifest, exons_nirvana,
        g2t
    )

    subprocess.check_call(command, shell=True)

    return vcf2xls_applet_out_dir


def run_reanalysis(input_dir, reanalysis_list):
    reanalysis_dict = {}

    with open(reanalysis_list) as r_fh:
        for line in r_fh:
            fields = line.strip().split("\t")
            assert len(fields) == 2, (
                "Unexpected number of fields in reanalysis_list. "
                "File must contain one tab separated "
                "sample/panel combination per line"
            )
            sample, panel = fields
            reanalysis_dict.setdefault(sample, set()).add(panel)

    run_vcf2xls_app(input_dir, reanalysis_dict)
