# dias.py

This Python module was developed by the East GLH bioinformatics team to run DNAnexus workflows as part of germline analysis of next generation sequencing data: from FASTQ to Excel variant workbooks, generating quality and coverage reports.
This READme describes the module architecture and should serve as a guide to future users and developers of the Dias pipeline.
To understand how to run it, get trained by a qualified member of the team and read the Dias manual.

## Function

Run components of the Dias pipeline stored in DNAnexus as apps or workflows. This module collects the aproppriate input files for each stage in the workflows, sets off the jobs on DNAnexus and direct the output files into folders in a predefined structure.


It curently supports the following workflows and apps:
* dias_single_workflow: ran once per sample on a sequencing run
* dias_multi_workflow: ran once per sequencing run
* qc: the MultiQC app is ran once per sequencing run
* cnvcall: GATK's germline CNV calling steps are ran in cohort mode (ran once per sequencing run)
* dias_reports_workflow: ran once per sample
* dias_cnvreports_workflow: ran once per sample

## Files

- dias.py
  - Main script, called to run every part of the pipeline
- general_functions.py
  - Contains general purpose functions: get the date, create folders, get file ids...
- single_workflow.py
  - Generates the batch tsv and runs the dx cmd to start the single workflow
- multi_workflow.py
  - Generates the batch tsv and runs the dx cmd to start the multi workflow
- multiqc.py
  - Runs the MultiQC app to generate a run-level quality report
- cnvcalling.py
  - Runs the GATK_gCNV app for run-level CNVcalling, with samples listed in the input file excluded
- reports.py
  - Generate batch files for reports workflow and reanalysis requests
- cnvreports.py
  - Generate batch files for CNV reports workflow and CNV reanalysis requests
