ref_project_id = "project-Fkb6Gkj433GVVvj73J7x8KbV"

# Single workflow

ss_workflow_id = "{}:workflow-Fx13Gj8433GpvFf4Kg0535bK".format(ref_project_id)
sentieon_R1_input_stage = "stage-Fk9p4Kj4yBGfpvQf85fQXJq5.reads_fastqgzs"
sentieon_R2_input_stage = "stage-Fk9p4Kj4yBGfpvQf85fQXJq5.reads2_fastqgzs"
sentieon_sample_input_stage = "stage-Fk9p4Kj4yBGfpvQf85fQXJq5.sample"
fastqc_fastqs_input_stage = "stage-Fx13V7j433GjFxbX2XxzYJVY.fastqs"

# Multi workflow

stage_input_dict = {
    "stage-FpPQpk8433GZz7615xq3FyvF.flagstat": {
        "app": "flagstat", "pattern": "flagstat$"
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

mqc_applet_id = "{}:applet-Fx7vgBQ433GpF2Xy4xQ55j6P".format(ref_project_id)
mqc_config_file = "{}:file-Fx32KXj433GkVgk83fv6Zjxx".format(ref_project_id)

# Vcf2xls

vcf2xls_applet_id = "{}:applet-Fx2Xgbj433GkgZ977ZFv8vZP".format(ref_project_id)
exons_nirvana = "{}:file-Fq18Yp0433GjB7172630p9Yv".format(ref_project_id)
