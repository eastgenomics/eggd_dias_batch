import sys

sys.path.append("/mnt/storage/apps/software/dias_config")

from dias_dynamic_files import (
    nirvana_genes2transcripts,
    bioinformatic_manifest,
    genepanels_file,
)

ref_project_id = "project-Fkb6Gkj433GVVvj73J7x8KbV"

# Dynamic files

g2t = nirvana_genes2transcripts
bio_manifest = bioinformatic_manifest
genepanels = genepanels_file

# Single workflow

ss_workflow_id = "{}:workflow-Fx13Gj8433GpvFf4Kg0535bK".format(ref_project_id)
sentieon_R1_input_stage = "stage-Fk9p4Kj4yBGfpvQf85fQXJq5.reads_fastqgzs"
sentieon_R2_input_stage = "stage-Fk9p4Kj4yBGfpvQf85fQXJq5.reads2_fastqgzs"
sentieon_sample_input_stage = "stage-Fk9p4Kj4yBGfpvQf85fQXJq5.sample"
fastqc_fastqs_input_stage = "stage-Fx13V7j433GjFxbX2XxzYJVY.fastqs"

# Multi workflow

happy_stage_prefix = "stage-Fq1BPKj433Gx3K4Y8J35j0fv.prefix"

stage_input_dict = {
    "stage-FpPQpk8433GZz7615xq3FyvF.flagstat": {
        "app": "flagstat", "subdir": "", "pattern": "flagstat$"
    },
    "stage-FpPQpk8433GZz7615xq3FyvF.coverage": {
        "app": "region_coverage", "subdir": "", "pattern": "5bp.gz$",
    },
    "stage-FpPQpk8433GZz7615xq3FyvF.coverage_index": {
        "app": "region_coverage", "subdir": "", "pattern": "5bp.gz.tbi$",
    },
    "stage-Fpz3Jqj433Gpv7yQFfKz5f8g.SampleSheet": {
        "app": None, "subdir": "", "pattern": "SampleSheet.csv$",
    },
    "stage-Fq1BPKj433Gx3K4Y8J35j0fv.query_vcf": {
        "app": "sentieon-dnaseq", "subdir": "",
        "pattern": "NA12878_markdup_recalibrated_Haplotyper.vcf.gz$",
    },
}

ms_workflow_id = "{}:workflow-FpKqKP8433Gj8JbxB0433F3y".format(ref_project_id)

# MultiQC

mqc_applet_id = "{}:applet-Fxjpx8j433GZKqkG1vZp8vk6".format(ref_project_id)
mqc_config_file = "{}:file-FxjpvF8433GvjKFVK6k77G4q".format(ref_project_id)

# Vcf2xls

vcf2xls_applet_id = "{}:applet-Fxjp588433GQ0V6b2bGYQz05".format(ref_project_id)
exons_nirvana = "{}:file-Fq18Yp0433GjB7172630p9Yv".format(ref_project_id)
