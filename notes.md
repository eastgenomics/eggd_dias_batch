# TO DELETE AT SOME POINT LATER WHEN THIS IS ALL OVER

## TO FIGURE OUT
- what to do with tests not in genepanels   
    - atm just exclude from manifest
- what to do with samples with no required files (i.e vcfs)
    - what if they're all missing
- what to do with 
- anything weird with samplenames to handle
- need to check requested HGNC IDs are valid?
- format of summary file with record of whats run
- structure of output dirs
- naming of output files
- passing job ID vs output path of cnv call


# Running cnv reports

# PLAN
- functions needed:
    - find_dx_files ###DONE###
        - give it file pattern, single dir and some other part of path
    - filter_manifest_against_file_list  DONE
    - get_ci_and_panels_from_test_codes
        - need to go over all test codes and build list for each sample of test code, panel and CI strings
        - this will be 1+ lists of each for each sample depending on no. reports to generate



## Epic manifest
- gather *segments.vcf files from cnv calling
- generate list of sample names that have cnv vcfs
- generate sample -> panel and CI dict
    - use first 2 parts of vcf filename and check against manifest that they in manifest sample list
    - subset the manifest against which there are vcfs (I think)
    - loop over sample - test_code dict
        - loop over test codes
            - building 3 lists: CIs (R or C test code or HGNC ID), panels (panel name)  and 'prefixes' (test code of HGNC ID)
            - HGNC -> append to all 3
            - filter through gene panels by test code up to '_' from first column of gene panels file
            - if no match -> chuck out as invalid test
            - match
                - CI -> append full CI string from gene panels 1st col
                - panel -> add panel name for the CI
                - prefixes -> add original test code (i.e. C1.1)
            - ^ build list of above for any samples missing vcfs or booked for tests that don't exist

## Gemini manifest
- gather *segments.vcf files from cnv calling
- manifest is partial identifiers
- loop over cnv files
    - get first part of name '-'
        - # note to me - can just do a .startswith on cnv filenames for both epic and gemini (maybe a re.match with a trailing - or . to stop matching others whnere the name is a subset)
    - check if its in manifest
- filter the manifest dict down to just those that have a cnv file
- check for CI prefix against prefixes of genepanels col 1
  - chuck out no matches as invalid
- match
    - CI -> append full CI string from gene panels 1st col
    - panel -> add panel name for the CI
    - prefixes -> add original test code (i.e. C1.1)
- ^ build list of above for any samples missing vcfs or booked for tests 

## Other CNV reports parts
- get inputs for reports workflow stages
    - uses:
        - cnv call output dir
        - list of sample names
        - dict mapping of input field -> pattern to use to search
                # eggd_vep
                "{}.vcf".format(cnv_vep_stage_id): {
                    "app": "eggd_GATKgCNV_call", "subdir": "CNV_vcfs/",
                    "pattern": "-E '{}(.*)_segments.vcf$'"
                },
                # excluded_annotate
                "{}.excluded_regions".format(cnv_annotate_excluded_regions_stage_id): {
                    "app": "eggd_GATKgCNV_call", "subdir": "CNV_summary/",
                    "pattern": "-E '(.*)_excluded_intervals.bed$'"
                }
    - returns dict with sample -> inputs formatted to run
- set inputs for:
    - workbooks
        - clinical indication str
        - panel str
    - generate_bed(s)
        - panel str
        - output prefix

- CIs -> ';' joined
- panels -> ';' joined
- test_codes -> '&&' joined

<!-- ~~~ run ~~~ -->
    


# Running SNV reports


- find sentieon output dir from single output ('sentieon')
- find single vcfs (vcf.gz)
- get list of sample names of vcfs (by splitting on `_`)

## Epic manifest
- use first 2 parts of vcf name to check if its in a manifest sample
    - if it is add the filename to that sample
- filter down sample - test code dict for ones that have vcfs
- loop over sample - test code dict
    - test code is gene ->
        - append to CI, panels and prefixes list
    - test code is panel ->
        - get clinical indication from 'short' R/C code by splitting on '_'
        - if found:
            - ci list -> append full gene panels CI entry
            - panels -> append full panel name
            - prefixes -> append short code
        - else:
            - dump out as invalid code

## Gemini manifest
- loop over single sample vcfs
    - split on '-' (assume this leaves just X no.)
    - if sample in manifest samples -> add filename to that sample
- filter down sample -> test code dict for those with a vcf
- loop over manifest samples
    - if CI is gene
        - append to all 3 lists
    - if CI is a panel
        - match against test code
    