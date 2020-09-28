# dias.py

This readme is mainly to help understand what's going on. To understand how to run it, get trained by a qualified member of the team or read the Dias manual in Q-Pulse

## Function

Run the Dias pipeline stored in DNAnexus

## Files

- dias.py
  - Main script, called to run every part of the pipeline
- config.py
  - Contains the apps id, stage ids.. Also contains the importing of the dias_config file
- general_functions.py
  - Contains general purpose functions: get the date, create folders, get file ids...
- single_workflow.py
  - Generates the batch tsv and runs the dx cmd to start the single workflow
- multi_workflow.py
  - Generates the batch tsv and runs the dx cmd to start the multi workflow
- multiqc.py
  - Runs the multiqc
- vcf2xls.py
  - Generate batch files for vcf2xls, reports and reanalysis
