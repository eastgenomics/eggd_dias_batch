# dias.py

This Python module was developed by the East GLH rare disease bioinformatics team to run DNAnexus apps and workflows as part of germline analysis of next generation sequencing data: CNV calling from BAMs, generate Excel variant workbooks from VCF and generating panel-specific coverage reports.
This READme describes the module architecture and should serve as a guide to future users and developers of the Dias pipeline.
To understand how to run it, get trained by a qualified member of the team and read the Dias manual.


## Setup
This module is written entirely in Python and is ran routinely in a Python 2.7 environment. Dependencies are listed in the requirements.txt.

To set up a testing environment in a Linux-based system, please follow the steps below. Prerequisites include Python 2.7, pip and virtualenv installed, as well as having a local copy of an assay-specific Dias config file.
```
mkdir ~/virtual_envs/Python2.7_Dias
python2 -m virtualenv ~/virtual_envs/Python2.7_Dias
source ~/virtual_envs/Python2.7_Dias/bin/activate
pip install -r requirements.txt
```


## Use case

Run components of the Dias pipeline stored in DNAnexus as apps or workflows. This module collects the appropriate input files for each stage in the workflows, sets off the jobs on DNAnexus and directs the output files into folders in a predefined structure.
It curently supports the following workflows and apps:
* GATKgCNV_call: GATK's germline CNV calling steps are ran in cohort mode (ran once per sequencing run)
* dias_reports_workflow: ran once per sample on a sequencing run
* dias_cnvreports_workflow: ran once per sample on a sequencing run

## Files

- dias.py
  - Main script, called to run every part of the pipeline
- general_functions.py
  - Contains general purpose functions: get the date, create folders, get file ids...
- cnvcalling.py
  - Runs the GATKgCNV_call app for run-level CNV detection
- reports.py
  - Generate batch files to create SNV reports using the dias_reports workflow, based on samples and panels provided in an Epic-style manifest or a Gemini-style reanalysis.tsv
- cnvreports.py
  - Generate batch files to create CNV reports using the dias_cnvreports workflow, based on samples and panels provided in an Epic-style manifest or a Gemini-style reanalysis.tsv
