# eggd_dias_batch (DNAnexus Platform App)

## What are typical use cases for this app?

DNAnexus app for launching SNV, CNV and mosaic reports workflow from a given directory of Dias single output and a manifest file.


## What inputs are required for this app to run?

**Required**
- `-iassay` (`str`): string of assay to run analysis for (CEN or TWE), used for searching of config files automatically (if `-iassay_config_file` not specified)
- `-iassay_config_file` (`file`): Config file for assay, if not provided will search assay_config_dir for files (using `-iassay`)
- `-isingle_output_dir` (`str`): path to output directory of Dias single to use as input files
- `-imanifest_file` (`file`): manifest file from Epic or Gemini, maps sample ID -> required test codes / HGNC IDs (required for running any reports mode)

**Useful ones**

- `-icnv_call_job_id` (`str`): job ID of cnv calling job to use for generating CNV reports if CNV calling is not first being run
- `-iexclude_samples` (`str`): comma separated string of samples to exclude from CNV calling / CNV reports
- `-iexclude_samples_file` (`file`): file of samples to exclude from CNV calling / CNV reports, one sample name per line (as found in manifest)
- `-isplit_tests` (`bool`): controls if to split multiple panels / genes in a manifest to individual reports instead of being combined into one


**Running modes**
- `-icnv_call` (`bool`): controls if to run CNV calling
- `-icnv_reports` (`bool`): controls if to run CNV reports workflows
- `-isnv_reports` (`bool`): controls if to run SNV reports workflows
- `-imosaic_reports` (`bool`): controls if to run mosaic reports workflow

**Testing**
- `-itesting` (`bool`): controls if to run in testing mode and terminate all launched jobs after launching
- `-isample_limit` (`int`): no. of samples to launch jobs for, used during testing to speed up running of app


## How does this app work?

The app takes as a minimum input a path to Dias single output and an assay config. The assay config file may be passed directly (with `-iassay_config_file`) or an assay string specified to run for (with `-iassay`) which will search DNAnexus for the highest version config file and use this for analysis. If running a reports workflow a manifest file must also be specified.

The general behaviour of each mode is as follows:

### CNV calling

**Minimum inputs**:
- `-iassay` or `-iassay_config_file`
- `-isingle_output_dir`

**Behaviour**:
- Check if inputs provided are valid
- Search for and download latest config for assay (if not provided directly)
- Parse through config file to add reference files to input fields
- Download and format genepanels file
- Search for bam files using the folder and name provided in the config file under `-isingle_output_dir`
    - Remove any files belonging to samples specified to `-iexclude_samples`
- Run CNV calling app
    - n.b. if `-icnv_reports=true` is specified, the app will be held until CNV calling completes, and the output will be used for launching CNV reports
- Launch reports workflow (if any specified; see below)
- Write summary report and upload

### Reports workflows

**Minimum inputs**:
- `-iassay` or `-iassay_config_file`
- `-isingle_output_dir`
- `-imanifest_file`
- `-icnv_reports` -> `-icnv_call=true` OR `-icnv_call_job_id`

**Behaviour**:
- Check if inputs provided are valid
- Search for and download latest config for assay (if not provided directly)
- Parse through config file to add reference files to input fields
- Download and format genepanels file
- Download manifest
    - Check provided test codes are valid and present in genepanels file
    - Get full panel and clinical indication strings for each test code from genepanels file
- For **CNV** reports:
    - Gather all `segments.vcf` files from CNV call job output
    - Find excluded intervals bed file from CNV call job output
    - Find previous xlsx reports (used for setting report name suffix)
    - Filter manifest by samples having a VCF found
    - Read CNV reports inputs from config, parse in string inputs (i.e. panel str to workbooks) and add vcf input for VEP
    - Check for previous xlsx reports for same sample and increment to always be +1
    - Launch CNV reports workflow
- For **SNV/mosaic** reports:
    - Gather all VCF and mosdepth files from sub dir and name pattern specified in config as input to VEP and Athena, respectively
    - Find previous xlsx reports (used for setting report name suffix)
    - Filter manifest by samples having VCF and mosdepth files found
    - Read SNV/mosaic reports inputs from config, parse in string inputs (i.e. panel str to workbooks), add VCF input for VEP and mosdepth files for Athena
    - Check for previous xlsx reports for same sample and increment to always be +1
    - Launch SNV/mosaic reports workflow

n.b.
- if `-itesting=true` is specified, reports jobs will launch but not start running, and will be automatically terminated on the app completing


### Example commands

Running CNV calling and CNV reports for CEN assay:
```
dx run app-eggd_dias_batch \
    -iassay=CEN \
    -imanifest_file=file-xxx \
    -isingle_output_dir=project-xxx:/path_to_output/ \
    -icnv_call=true \
    -icnv_reports=true
```

Running reports for CNV and SNV (using previous CNV calling output):
```
dx run app-eggd_dias_batch \
    -iassay=CEN \
    -imanifest_file=file-xxx \
    -isingle_output_dir=project-xxx:/path_to_output/ \
    -icnv_call_job_id=job-xxx \
    -icnv_reports=true \
    -isnv_reports=true
```

Running SNV reports with specified config file:
```
dx run app-eggd_dias_batch \
    -iassay_config_file=file-xxx \
    -imanifest_file=file-xxx \
    -isingle_output_dir=project-xxx:/path_to_output/ \
    -isnv_reports=true
```

Running all modes in testing:
```
dx run app-eggd_dias_batch \
    -iassay=CEN \
    -imanifest_file=file-xxx \
    -isingle_output_dir=project-xxx:/path_to_output/ \
    -icnv_call=true \
    -icnv_reports=true \
    -isnv_reports=true \
    -imosaic_reports=true
```


## Config file design

The config file for an assay is written in JSON format and specifies the majority of inputs for running each type of analysis. A populated example config file may be found [here](example/dias_batch_example_config.json).

The top level section should be structured as follows:
```
{
    "assay": "CEN",
    "version": "2.2.0",
    "cnv_call_app_id": "app-GJZVB2840KK0kxX998QjgXF0",
    "snv_report_workflow_id": "workflow-GXzkfYj4QPQp9z4Jz4BF09y6",
    "cnv_report_workflow_id": "workflow-GXzvJq84XZB1fJk9fBfG88XJ",
    "reference_files": {
        "genepanels": "project-Fkb6Gkj433GVVvj73J7x8KbV:file-GVx0vkQ433Gvq63k1Kj4Y562",
        "exons_nirvana": "project-Fkb6Gkj433GVVvj73J7x8KbV:file-GF611Z8433Gk7gZ47gypK7ZZ",
        "genes2transcripts": "project-Fkb6Gkj433GVVvj73J7x8KbV:file-GV4P970433Gj6812zGVBZvB4",
        "exonsfile": "project-Fkb6Gkj433GVVvj73J7x8KbV:file-GF611Z8433Gf99pBPbJkV7bq"
    },
    ...
```
- `assay` (`str`) : assay type the config is for, used  for finding highest version config file when `-iassay` is specified
- `version` (`str`) : the version of this config file
- `{cnv_call_app|_report_workflow}_id` (`str`) : the IDs of CNV calling and reports workflows to use
- `reference_files` (`dict`) : mapping of reference file name : DNAnexus file ID, reference file name _must_ be given as shown above, and DNAnexus file ID should be provided as `project-xxx:file-xxx`

The definitions of inputs for CNV calling and each reports workflow should be defined under the key `modes`, containing a mapping of all inputs and other inputs for controlling running of analyses.

**Example format of CNV call app structure**:
```
"modes": {
    "cnv_call": {
        "instance_type": "mem2_ssd1_v2_x8",
        "inputs": {
            "bambais": {
                "folder": "/sentieon-dnaseq-4.2.1/",
                "name": ".bam$|.bam.bai$"
            },
            "GATK_docker": {
                "$dnanexus_link": {
                    "$dnanexus_link": {
                        "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
                        "id": "file-GBBP9JQ433GxV97xBpQkzYZx"
                    }
                }
            },
            "annotation_tsv": {
                ...
```
- `instance_type` (`str`; optional) : instance type to use when running CNV calling app
- `inputs` (`dict`) : mapping of each app input field to required input
    - `bambais` is a dynamic input and BAM files are parsed at run time using the `folder` and `name` keys, `folder` will be used as a sub folder under the `-isingle_output_dir` specified, and `name` will be used as a regex pattern for finding files
    - other inputs should be specified in the standard `$dnanexus_link` mapping format to be passed directly to the underlying run API call


**Example format of a reports workflow structure**:
```
"cnv_reports": {
        "instance_type": {
            "stage-cnv_vep.vcf": "mem2_ssd2_v2_x72"
        },
        "inputs": {
            "stage-cnv_generate_bed_vep.exons_nirvana": "INPUT-exons_nirvana",
            "stage-cnv_generate_bed_vep.nirvana_genes2transcripts": "INPUT-genes2transcripts",
            "stage-cnv_generate_bed_vep.gene_panels": "INPUT-genepanels",
            "stage-cnv_generate_bed_vep.flank": 495,
            "stage-cnv_generate_bed_vep.additional_regions": {
                "$dnanexus_link": {
                    "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
                    "id": "file-GJZQvg0433GkyFZg13K6VV6p"
                }
            },
            "stage-cnv_vep.config_file": {
                "$dnanexus_link": {
                    "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
                    "id": "file-GQGJ3Z84xyx0jp1q65K1Q1jY"
                }
            },
            "stage-cnv_vep.vcf": {
                "folder": "CNV_vcfs",
                "name": "_segments.vcf$"
            },
```
- `instance_type` (`dict`; optional) : mapping of stage-name to instance type to use, this will override the app and workflow defaults
- `inputs` (`dict`) : mapping of each stage input field to required input
    - inputs may be defined as regular integers / strings / booleans, `$dnanexus_link` file mappings or using `INPUT-` placeholders
    - `INPUT-` placeholders are followed by a reference key from the `reference_files` mapping in the top level of the config file, and are parsed at run time into the inputs for the workflow (i.e. use of `"stage-cnv_generate_bed_vep.gene_panels": "INPUT-genepanels"` would result be replace by `project-Fkb6Gkj433GVVvj73J7x8KbV:file-GVx0vkQ433Gvq63k1Kj4Y562`, correctly formatted as a `$dnanexus_link` mapping)
    - inputs for the following stages follow the same behaviour as the `bambais` input for the CNV calling app of being provided as a "folder" and "name" key for searching:
        - cnv_reports: 
            - `stage-cnv_vep.vcf`
        - snv_reports and mosaic_reports
            - `stage-rpt_vep.vcf`
            - `stage-rpt_athena.mosdepth_files`
    - example format of the above:
        ```
        # example of finding VCFs from Sentieon
        # this will find any vcf|vcf.gz but NOT .g.vcf
        "stage-rpt_vep.vcf" :{
            "folder": "sentieon-dnaseq",
            "name": "^[^\.]*(?!\.g)\.vcf(\.gz)?$"
        }
        ```


## What does this app output

- `summary_report` (`file`) - text summary file with details on jobs run and any samples / tests excluded from analysis

