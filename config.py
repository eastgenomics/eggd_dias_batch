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

ss_workflow_id = "{}:workflow-Fy6k2pQ433Ggbb3BG271fp29".format(ref_project_id)
sentieon_R1_input_stage = "stage-Fy6fpk040vZZPPbq96Jb2KfK.reads_fastqgzs"
sentieon_R2_input_stage = "stage-Fy6fpk040vZZPPbq96Jb2KfK.reads2_fastqgzs"
sentieon_sample_input_stage = "stage-Fy6fpk040vZZPPbq96Jb2KfK.sample"
fastqc_fastqs_input_stage = "stage-Fy6fpV840vZZ0v6J8qBQYqZF.fastqs"

# Multi workflow

happy_stage_prefix = "stage-Fq1BPKj433Gx3K4Y8J35j0fv.prefix"

multi_stage_input_dict = {
    "stage-Fybykxj433GV7vJKFGf3yVkK.SampleSheet": {
        "app": None, "subdir": "", "pattern": "SampleSheet.csv$",
    },
    "stage-Fq1BPKj433Gx3K4Y8J35j0fv.query_vcf": {
        "app": "sentieon-dnaseq", "subdir": "",
        "pattern": "NA12878_markdup_recalibrated_Haplotyper.vcf.gz$",
    },
}

ms_workflow_id = "{}:workflow-FyQ2Gy0433Gz76Jp9j5YG80K".format(ref_project_id)

# MultiQC

mqc_applet_id = "{}:applet-Fxjpx8j433GZKqkG1vZp8vk6".format(ref_project_id)
mqc_config_file = "{}:file-FxjpvF8433GvjKFVK6k77G4q".format(ref_project_id)

# Reports

exons_nirvana = "{}:file-Fq18Yp0433GjB7172630p9Yv".format(ref_project_id)

rpt_workflow_id = "{}:workflow-Fyq5yf8433GzxPK360B8Qff0".format(ref_project_id)

rpt_stage_input_dict = {
    # vcf2xls
    "stage-Fyq5ypj433GzxPK360B8Qfg5.annotated_vcf": {
        "app": "nirvana2vcf", "subdir": "",
        "pattern": "-E '{}(.*).annotated.vcf$'"
    },
    "stage-Fyq5ypj433GzxPK360B8Qfg5.raw_vcf": {
        # pattern excludes "g" because g.vcf are in the same folder
        "app": "sentieon-dnaseq", "subdir": "",
        "pattern": "-E '{}(.*)[^g].vcf.gz$'"
    },
    "stage-Fyq5ypj433GzxPK360B8Qfg5.sample_coverage_file": {
        "app": "region_coverage", "subdir": "",
        "pattern": "-E '{}(.*)5bp.gz$'",
    },
    "stage-Fyq5ypj433GzxPK360B8Qfg5.sample_coverage_index": {
        "app": "region_coverage", "subdir": "",
        "pattern": "-E '{}(.*)5bp.gz.tbi$'",
    },
    "stage-Fyq5ypj433GzxPK360B8Qfg5.flagstat_file": {
        "app": "flagstat", "subdir": "", "pattern": "-E '{}(.*)flagstat$'"
    },
    # generate_bed
    "stage-Fyq5yy0433GXxz691bKyvjPJ.sample_file": {
        "app": "mosdepth", "subdir": "",
        "pattern": "-E '{}(.*).per-base.bed.gz.csi$'"
    },
    # athena
    "stage-Fyq5z18433GfYZbp3vX1KqjB.mosdepth_files": {
        "app": "mosdepth", "subdir": "",
        # athena requires both per-base files and reference files
        "pattern": "-E '{}(.*)(per-base.bed.gz$|reference)'"
    },
}

rpt_dynamic_files = {
    "stage-Fyq5ypj433GzxPK360B8Qfg5.genepanels_file ID": genepanels_file,
    "stage-Fyq5ypj433GzxPK360B8Qfg5.genepanels_file": "",
    "stage-Fyq5ypj433GzxPK360B8Qfg5.bioinformatic_manifest ID": bioinformatic_manifest,
    "stage-Fyq5ypj433GzxPK360B8Qfg5.bioinformatic_manifest": "",
    "stage-Fyq5ypj433GzxPK360B8Qfg5.nirvana_genes2transcripts ID": nirvana_genes2transcripts,
    "stage-Fyq5ypj433GzxPK360B8Qfg5.nirvana_genes2transcripts": "",
    "stage-Fyq5yy0433GXxz691bKyvjPJ.exons_nirvana ID": exons_nirvana,
    "stage-Fyq5yy0433GXxz691bKyvjPJ.exons_nirvana": "",
    "stage-Fyq5yy0433GXxz691bKyvjPJ.nirvana_genes2transcripts ID": nirvana_genes2transcripts,
    "stage-Fyq5yy0433GXxz691bKyvjPJ.nirvana_genes2transcripts": "",
    "stage-Fyq5yy0433GXxz691bKyvjPJ.gene_panels ID": genepanels_file,
    "stage-Fyq5yy0433GXxz691bKyvjPJ.gene_panels": "",
    "stage-Fyq5yy0433GXxz691bKyvjPJ.manifest ID": bioinformatic_manifest,
    "stage-Fyq5yy0433GXxz691bKyvjPJ.manifest": "",
    "stage-Fyq5z18433GfYZbp3vX1KqjB.exons_nirvana ID": exons_nirvana,
    "stage-Fyq5z18433GfYZbp3vX1KqjB.exons_nirvana": ""
}

# reanalysis

rea_stage_input_dict = {
    # vcf2xls
    "stage-Fyq5ypj433GzxPK360B8Qfg5.annotated_vcf": {
        "app": "nirvana2vcf", "subdir": "",
        "pattern": "-E '{}(.*).annotated.vcf$'"
    },
    "stage-Fyq5ypj433GzxPK360B8Qfg5.raw_vcf": {
        # pattern excludes "g" because g.vcf are in the same folder
        "app": "sentieon-dnaseq", "subdir": "",
        "pattern": "-E '{}(.*)[^g].vcf.gz$'"
    },
    "stage-Fyq5ypj433GzxPK360B8Qfg5.sample_coverage_file": {
        "app": "region_coverage", "subdir": "",
        "pattern": "-E '{}(.*)5bp.gz$'",
    },
    "stage-Fyq5ypj433GzxPK360B8Qfg5.sample_coverage_index": {
        "app": "region_coverage", "subdir": "",
        "pattern": "-E '{}(.*)5bp.gz.tbi$'",
    },
    "stage-Fyq5ypj433GzxPK360B8Qfg5.flagstat_file": {
        "app": "flagstat", "subdir": "", "pattern": "-E '{}(.*)flagstat$'"
    },
    # athena
    "stage-Fyq5z18433GfYZbp3vX1KqjB.mosdepth_files": {
        "app": "mosdepth", "subdir": "",
        # athena requires both per-base files and reference files
        "pattern": "-E '{}(.*)(per-base.bed.gz$|reference)'"
    },
}

rea_dynamic_files = {
    "stage-Fyq5ypj433GzxPK360B8Qfg5.genepanels_file ID": genepanels_file,
    "stage-Fyq5ypj433GzxPK360B8Qfg5.genepanels_file": "",
    "stage-Fyq5ypj433GzxPK360B8Qfg5.bioinformatic_manifest ID": bioinformatic_manifest,
    "stage-Fyq5ypj433GzxPK360B8Qfg5.bioinformatic_manifest": "",
    "stage-Fyq5ypj433GzxPK360B8Qfg5.nirvana_genes2transcripts ID": nirvana_genes2transcripts,
    "stage-Fyq5ypj433GzxPK360B8Qfg5.nirvana_genes2transcripts": "",
    "stage-Fyq5yy0433GXxz691bKyvjPJ.exons_nirvana ID": exons_nirvana,
    "stage-Fyq5yy0433GXxz691bKyvjPJ.exons_nirvana": "",
    "stage-Fyq5yy0433GXxz691bKyvjPJ.nirvana_genes2transcripts ID": nirvana_genes2transcripts,
    "stage-Fyq5yy0433GXxz691bKyvjPJ.nirvana_genes2transcripts": "",
    "stage-Fyq5yy0433GXxz691bKyvjPJ.gene_panels ID": genepanels_file,
    "stage-Fyq5yy0433GXxz691bKyvjPJ.gene_panels": "",
    "stage-Fyq5z18433GfYZbp3vX1KqjB.exons_nirvana ID": exons_nirvana,
    "stage-Fyq5z18433GfYZbp3vX1KqjB.exons_nirvana": ""
}
