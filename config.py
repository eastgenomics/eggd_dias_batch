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

ss_workflow_id = "{}:workflow-G0QfxJ0433GyPP9b1VKYZYBX".format(ref_project_id)
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

mqc_applet_id = "{}:applet-Fz93FfQ433Gvf6pKFZYbXZQf".format(ref_project_id)
mqc_config_file = "{}:file-Fz947KQ433GbpkvQ104ybPVg".format(ref_project_id)

# Reports

exons_nirvana = "{}:file-Fq18Yp0433GjB7172630p9Yv".format(ref_project_id)

vcf2xls_stage_id = "stage-Fyq5ypj433GzxPK360B8Qfg5"
generate_bed_stage_id = "stage-Fyq5yy0433GXxz691bKyvjPJ"
athena_stage_id = "stage-Fyq5z18433GfYZbp3vX1KqjB"

rpt_workflow_id = "{}:workflow-G192V6j433GVv7PQKB6X04xV".format(ref_project_id)

rpt_stage_input_dict = {
    # vcf2xls
    "{}.annotated_vcf".format(vcf2xls_stage_id): {
        "app": "nirvana2vcf", "subdir": "",
        "pattern": "-E '{}(.*).annotated.vcf$'"
    },
    "{}.raw_vcf".format(vcf2xls_stage_id): {
        # pattern excludes "g" because g.vcf are in the same folder
        "app": "sentieon-dnaseq", "subdir": "",
        "pattern": "-E '{}(.*)[^g].vcf.gz$'"
    },
    "{}.sample_coverage_file".format(vcf2xls_stage_id): {
        "app": "region_coverage", "subdir": "",
        "pattern": "-E '{}(.*)5bp.gz$'",
    },
    "{}.sample_coverage_index".format(vcf2xls_stage_id): {
        "app": "region_coverage", "subdir": "",
        "pattern": "-E '{}(.*)5bp.gz.tbi$'",
    },
    "{}.flagstat_file".format(vcf2xls_stage_id): {
        "app": "flagstat", "subdir": "", "pattern": "-E '{}(.*)flagstat$'"
    },
    # generate_bed
    "{}.sample_file".format(generate_bed_stage_id): {
        "app": "mosdepth", "subdir": "",
        "pattern": "-E '{}(.*).per-base.bed.gz.csi$'"
    },
    # athena
    "{}.mosdepth_files".format(athena_stage_id): {
        "app": "mosdepth", "subdir": "",
        # athena requires both per-base files and reference files
        "pattern": "-E '{}(.*)(per-base.bed.gz$|reference)'"
    },
}

rpt_dynamic_files = {
    "{}.genepanels_file ID".format(vcf2xls_stage_id): genepanels_file,
    "{}.genepanels_file".format(vcf2xls_stage_id): "",
    "{}.bioinformatic_manifest ID".format(vcf2xls_stage_id): bioinformatic_manifest,
    "{}.bioinformatic_manifest".format(vcf2xls_stage_id): "",
    "{}.nirvana_genes2transcripts ID".format(vcf2xls_stage_id): nirvana_genes2transcripts,
    "{}.nirvana_genes2transcripts".format(vcf2xls_stage_id): "",
    "{}.exons_nirvana ID".format(generate_bed_stage_id): exons_nirvana,
    "{}.exons_nirvana".format(generate_bed_stage_id): "",
    "{}.nirvana_genes2transcripts ID".format(generate_bed_stage_id): nirvana_genes2transcripts,
    "{}.nirvana_genes2transcripts".format(generate_bed_stage_id): "",
    "{}.gene_panels ID".format(generate_bed_stage_id): genepanels_file,
    "{}.gene_panels".format(generate_bed_stage_id): "",
    "{}.manifest ID".format(generate_bed_stage_id): bioinformatic_manifest,
    "{}.manifest".format(generate_bed_stage_id): "",
    "{}.exons_nirvana ID".format(athena_stage_id): exons_nirvana,
    "{}.exons_nirvana".format(athena_stage_id): ""
}

# reanalysis

rea_stage_input_dict = {
    # vcf2xls
    "{}.annotated_vcf".format(vcf2xls_stage_id): {
        "app": "nirvana2vcf", "subdir": "",
        "pattern": "-E '{}(.*).annotated.vcf$'"
    },
    "{}.raw_vcf".format(vcf2xls_stage_id): {
        # pattern excludes "g" because g.vcf are in the same folder
        "app": "sentieon-dnaseq", "subdir": "",
        "pattern": "-E '{}(.*)[^g].vcf.gz$'"
    },
    "{}.sample_coverage_file".format(vcf2xls_stage_id): {
        "app": "region_coverage", "subdir": "",
        "pattern": "-E '{}(.*)5bp.gz$'",
    },
    "{}.sample_coverage_index".format(vcf2xls_stage_id): {
        "app": "region_coverage", "subdir": "",
        "pattern": "-E '{}(.*)5bp.gz.tbi$'",
    },
    "{}.flagstat_file".format(vcf2xls_stage_id): {
        "app": "flagstat", "subdir": "", "pattern": "-E '{}(.*)flagstat$'"
    },
    # athena
    "{}.mosdepth_files".format(athena_stage_id): {
        "app": "mosdepth", "subdir": "",
        # athena requires both per-base files and reference files
        "pattern": "-E '{}(.*)(per-base.bed.gz$|reference)'"
    },
}

rea_dynamic_files = {
    "{}.genepanels_file ID".format(vcf2xls_stage_id): genepanels_file,
    "{}.genepanels_file".format(vcf2xls_stage_id): "",
    "{}.bioinformatic_manifest ID".format(vcf2xls_stage_id): bioinformatic_manifest,
    "{}.bioinformatic_manifest".format(vcf2xls_stage_id): "",
    "{}.nirvana_genes2transcripts ID".format(vcf2xls_stage_id): nirvana_genes2transcripts,
    "{}.nirvana_genes2transcripts".format(vcf2xls_stage_id): "",
    "{}.exons_nirvana ID".format(generate_bed_stage_id): exons_nirvana,
    "{}.exons_nirvana".format(generate_bed_stage_id): "",
    "{}.nirvana_genes2transcripts ID".format(generate_bed_stage_id): nirvana_genes2transcripts,
    "{}.nirvana_genes2transcripts".format(generate_bed_stage_id): "",
    "{}.gene_panels ID".format(generate_bed_stage_id): genepanels_file,
    "{}.gene_panels".format(generate_bed_stage_id): "",
    "{}.exons_nirvana ID".format(athena_stage_id): exons_nirvana,
    "{}.exons_nirvana".format(athena_stage_id): ""
}
