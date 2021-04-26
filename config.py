import sys

sys.path.append("/mnt/storage/home/kimy/duty_stuff/dias/egg1_dias_TSO_config/")

from egg1_config import (
    # single workflow
    ss_workflow_id,
    sentieon_R1_input_stage,
    sentieon_R2_input_stage,
    sentieon_sample_input_stage,
    fastqc_fastqs_input_stage,
    ss_beds_inputs,
    # multi workflow
    multi_stage_input_dict,
    ms_workflow_id,
    happy_stage_bed,
    # multiqc
    mqc_applet_id,
    mqc_config_file,
    # reports workflow
    rpt_stage_input_dict,
    rpt_dynamic_files,
    rpt_workflow_id,
    rea_stage_input_dict,
    rea_dynamic_files,
    vcf2xls_stage_id,
    generate_bed_stage_id
)
