default_mode_file_patterns = {
    'cnv_reports': {
        'sample': [
            '_segments.vcf$'
        ],
        'run': [
            '_excluded_intervals.bed$'
        ]
    },
    'snv_reports': {
        'sample': [
            '_markdup_recalibrated_Haplotyper.vcf.gz$',
            'per-base.bed.gz$',
            'reference_build.txt$'
        ],
        'run': []
    },
    'mosaic_reports': {
        'sample': [
            '_markdup_recalibrated_tnhaplotyper2.vcf.gz',
            'per-base.bed.gz$',
            'reference_build.txt$'
        ],
        'run': []
    },
    'artemis': {
        'bam$',
        'bam.bai$',
        '_copy_ratios.gcnv.bed$',
        '_copy_ratios.gcnv.bed.tbi$'
    }
}