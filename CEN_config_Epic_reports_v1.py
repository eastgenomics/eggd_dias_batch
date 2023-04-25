import sys

# sys.path.append("/mnt/storage/apps/software/dias_config")

# from dias_dynamic_files import (
#     genes2transcripts,
#     bioinformatic_manifest,
#     genepanels_file,
# )

assay_name = "CEN" # Core Endo Neuro
assay_version = "EPICreports_v1"

ref_project_id = "project-Fkb6Gkj433GVVvj73J7x8KbV"

# genepanels 221027
genepanels_file = "{}:file-GJJ7Vx8433Gz96yp8V98X74f".format(ref_project_id)
# manifest 221027
bioinformatic_manifest = "{}:file-GJJ7xz0433GyJ7X2B4ybbFB1".format(ref_project_id)
# g2t 230123
genes2transcripts = "{}:file-GP7FY50433GZX7x0JqfgBB4q".format(ref_project_id)

# Reports

xlsx_flanks = 495

cds_file = "{}:file-GF611Z8433Gk7gZ47gypK7ZZ".format(ref_project_id)
cds_file_for_athena = "{}:file-GF611Z8433Gf99pBPbJkV7bq".format(ref_project_id)
vep_config = "{}:file-GQ2yZ7j45fVVVBJ86XBfz4x6".format(ref_project_id)

generate_bed_vep_stage_id = "stage-G9P8p104vyJJGy6y86FQBxkv"
vep_stage_id = "stage-G9Q0jzQ4vyJ3x37X4KBKXZ5v"
generate_workbook_stage_id = "stage-G9P8VQj4vyJBJ0kg50vzVPxY"
generate_bed_athena_stage_id = "stage-Fyq5yy0433GXxz691bKyvjPJ"
athena_stage_id = "stage-Fyq5z18433GfYZbp3vX1KqjB"

rpt_workflow_id = "{}:workflow-GBQ985Q433GYJjv0379PJqqg".format(ref_project_id)

rpt_stage_input_dict = {
    # generate_bed
    "{}.sample_file".format(generate_bed_athena_stage_id): {
        "app": "mosdepth", "subdir": "",
        "pattern": "-E '{}(.*).per-base.bed.gz.csi$'"
    },
    "{}.sample_file".format(generate_bed_vep_stage_id): {
        "app": "mosdepth", "subdir": "",
        "pattern": "-E '{}(.*).per-base.bed.gz.csi$'"
    },
    # vep
    "{}.vcf".format(vep_stage_id): {
        "app": "sentieon-dnaseq", "subdir": "",
        "pattern": "-E '{}(.*)[^g].vcf.gz$'"
    },
    # athena
    "{}.mosdepth_files".format(athena_stage_id): {
        "app": "mosdepth", "subdir": "",
        # athena requires both per-base files and reference files
        "pattern": "-E '{}(.*)(per-base.bed.gz$|reference)'"
    },
}

rpt_dynamic_files = {
    # inputs for generate bed for vep
    "{}.exons_nirvana ID".format(generate_bed_vep_stage_id): cds_file,
    "{}.exons_nirvana".format(generate_bed_vep_stage_id): "",
    "{}.nirvana_genes2transcripts ID".format(generate_bed_vep_stage_id): genes2transcripts,
    "{}.nirvana_genes2transcripts".format(generate_bed_vep_stage_id): "",
    "{}.gene_panels ID".format(generate_bed_vep_stage_id): genepanels_file,
    "{}.gene_panels".format(generate_bed_vep_stage_id): "",
    "{}.manifest ID".format(generate_bed_vep_stage_id): bioinformatic_manifest,
    "{}.manifest".format(generate_bed_vep_stage_id): "",
    # inputs for generate bed for athena
    "{}.exons_nirvana ID".format(generate_bed_athena_stage_id): cds_file,
    "{}.exons_nirvana".format(generate_bed_athena_stage_id): "",
    "{}.nirvana_genes2transcripts ID".format(generate_bed_athena_stage_id): genes2transcripts,
    "{}.nirvana_genes2transcripts".format(generate_bed_athena_stage_id): "",
    "{}.gene_panels ID".format(generate_bed_athena_stage_id): genepanels_file,
    "{}.gene_panels".format(generate_bed_athena_stage_id): "",
    "{}.manifest ID".format(generate_bed_athena_stage_id): bioinformatic_manifest,
    "{}.manifest".format(generate_bed_athena_stage_id): "",
    # inputs for athena
    "{}.exons_file ID".format(athena_stage_id): cds_file_for_athena,
    "{}.exons_file".format(athena_stage_id): ""
}

# reanalysis

rea_stage_input_dict = {
    # vep
    "{}.vcf".format(vep_stage_id): {
        "app": "sentieon-dnaseq", "subdir": "",
        "pattern": "-E '{}(.*)[^g].vcf.gz$'"
    },
    # athena
    "{}.mosdepth_files".format(athena_stage_id): {
        "app": "mosdepth", "subdir": "",
        # athena requires both per-base files and reference files
        "pattern": "-E '{}(.*)(per-base.bed.gz$|reference)'"
    },
}

rea_dynamic_files = {
    # inputs for generate bed for vep
    "{}.exons_nirvana ID".format(generate_bed_vep_stage_id): cds_file,
    "{}.exons_nirvana".format(generate_bed_vep_stage_id): "",
    "{}.nirvana_genes2transcripts ID".format(generate_bed_vep_stage_id): genes2transcripts,
    "{}.nirvana_genes2transcripts".format(generate_bed_vep_stage_id): "",
    "{}.gene_panels ID".format(generate_bed_vep_stage_id): genepanels_file,
    "{}.gene_panels".format(generate_bed_vep_stage_id): "",
    # inputs for generate bed for athena
    "{}.exons_nirvana ID".format(generate_bed_athena_stage_id): cds_file,
    "{}.exons_nirvana".format(generate_bed_athena_stage_id): "",
    "{}.nirvana_genes2transcripts ID".format(generate_bed_athena_stage_id): genes2transcripts,
    "{}.nirvana_genes2transcripts".format(generate_bed_athena_stage_id): "",
    "{}.gene_panels ID".format(generate_bed_athena_stage_id): genepanels_file,
    "{}.gene_panels".format(generate_bed_athena_stage_id): "",
    # inputs for athena
    "{}.exons_file ID".format(athena_stage_id): cds_file_for_athena,
    "{}.exons_file".format(athena_stage_id): ""
}
