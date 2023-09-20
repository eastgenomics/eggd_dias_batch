# eggd_dias_batch (DNAnexus Platform App)

## What are typical use cases for this app?

DNAnexus app for launching SNV, CNV and mosaic reports workflow from a given directory of Dias single output and a manifest file.


## What inputs are required for this app to run?

- `-iassay` (`str`): string of assay to run analysis for (CEN or TWE), used for searching of config files
- `-iassay_config_file` (`file`): Config file for assay, if not provided will search assay_config_dir for files
- `-isingle_output_dir` (`str`): path to output directory of Dias single to use as input files
- `-imanifest_file` (`file`): manifest file from Epic or Gemini, maps sample ID -> required test codes / HGNC IDs
- `-isplit_tests` (`bool`): controls if to split multiple panels / genes in a manifest to individual reports instead of being combined into one
- `-icnv_call_job_id` (`str`): job ID of cnv calling job to use for generating CNV reports if CNV calling is not first being run
- `-iexclude_samples` (`str`): comma separated string of samples to exclude from analysis
- `-icnv_call` (`bool`): controls if to run CNV calling
- `-icnv_reports` (`bool`): controls if to run CNV reports workflows
- `-isnv_reports` (`bool`): controls if to run SNV reports workflows
- `-imosaic_reports` (`bool`): controls if to run mosaic reports workflow
- `-itesting` (`bool`): controls if to run in testing mode and terminate all launched jobs after launching
- `-isample_limit` (`int`): no. of samples to launch jobs for, used during testing to speed up running of app


## How does this app work?



## What does this app output


## Notes