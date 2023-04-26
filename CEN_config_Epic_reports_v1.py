assay_name = "CEN" # Core Endo Neuro
assay_version = "EPICreports_v2"

ref_project_id = "project-Fkb6Gkj433GVVvj73J7x8KbV"

### Dynamic files:

## for generate_bed
# genepanels 221027
genepanels_file = "{}:file-GJJ7Vx8433Gz96yp8V98X74f".format(ref_project_id)
# g2t 230123
genes2transcripts = "{}:file-GP7FY50433GZX7x0JqfgBB4q".format(ref_project_id)
# GCF_000001405.25_GRCh37.p13_genomic.exon_5bp_v2.0.0.tsv
exons_nirvana = "{}:file-GF611Z8433Gk7gZ47gypK7ZZ".format(ref_project_id)
# for generate_bed_for_VEP
vep_bed_flank = 495

## for eggd_Athena
# GCF_000001405.25_GRCh37.p13_genomic.symbols.exon_5bp_v2.0.0.tsv
exons_file = "{}:file-GF611Z8433Gf99pBPbJkV7bq".format(ref_project_id)

## for eggd_VEP
# VEP config file
vep_config = "{}:file-GQ2yZ7j45fVVVBJ86XBfz4x6".format(ref_project_id)


### Workflows

# dias_reports
# v2.0.4
rpt_workflow_id = "{}:workflow-GBQ985Q433GYJjv0379PJqqg".format(ref_project_id)

generate_bed_vep_stage_id = "stage-G9P8p104vyJJGy6y86FQBxkv"
vep_stage_id = "stage-G9Q0jzQ4vyJ3x37X4KBKXZ5v"
generate_workbook_stage_id = "stage-G9P8VQj4vyJBJ0kg50vzVPxY"
generate_bed_athena_stage_id = "stage-Fyq5yy0433GXxz691bKyvjPJ"
athena_stage_id = "stage-Fyq5z18433GfYZbp3vX1KqjB"

rpt_dynamic_files = {
    # inputs for generate bed for vep
    "{}.exons_nirvana ID".format(generate_bed_vep_stage_id): exons_nirvana,
    "{}.exons_nirvana".format(generate_bed_vep_stage_id): "",
    "{}.nirvana_genes2transcripts ID".format(generate_bed_vep_stage_id): genes2transcripts,
    "{}.nirvana_genes2transcripts".format(generate_bed_vep_stage_id): "",
    "{}.gene_panels ID".format(generate_bed_vep_stage_id): genepanels_file,
    "{}.gene_panels".format(generate_bed_vep_stage_id): "",
    # input for eggd_vep
    "{}.config_file ID".format(vep_stage_id): vep_config,
    "{}.config_file".format(vep_stage_id): "",
    # inputs for generate bed for athena
    "{}.exons_nirvana ID".format(generate_bed_athena_stage_id): exons_nirvana,
    "{}.exons_nirvana".format(generate_bed_athena_stage_id): "",
    "{}.nirvana_genes2transcripts ID".format(generate_bed_athena_stage_id): genes2transcripts,
    "{}.nirvana_genes2transcripts".format(generate_bed_athena_stage_id): "",
    "{}.gene_panels ID".format(generate_bed_athena_stage_id): genepanels_file,
    "{}.gene_panels".format(generate_bed_athena_stage_id): "",
    # inputs for athena
    "{}.exons_file ID".format(athena_stage_id): exons_file,
    "{}.exons_file".format(athena_stage_id): ""
}

# Sample-specific input files and their search patterns
rpt_stage_input_dict = {
    # eggd_vep
    "{}.vcf".format(vep_stage_id): {
        "app": "sentieon-dnaseq", "subdir": "",
        "pattern": "-E '{}(.*)[^g].vcf.gz$'"
    },
    # eggd_athena
    "{}.mosdepth_files".format(athena_stage_id): {
        "app": "mosdepth", "subdir": "",
        # athena requires both per-base files and reference files
        "pattern": "-E '{}(.*)(per-base.bed.gz$|reference)'"
    },
}
