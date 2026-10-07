"""
Functions for checking samples specified to exclude from running
"""
import re

import dxpy

from .formatting import prettier_print


def check_exclude_samples(samples, exclude, mode, single_dir=None) -> None:
    """
    Checks samples specified to either -iexclude_samples or
    -iexclude_samples_file are present in the manifest and/or have
    corresponding bams in the dias single dir.

    Parameters
    ----------
    samples : list
        list of sample names, will either be list of bam files found
        (before CNV calling) or sample names from manifest (if called
        from CNV reports)
    exclude : list[str]
        list of sample names to exclude from generating reports (n.b.
        this is ONLY for CNV reports), will be formatted as
        InstrumentID-SpecimenID (i.e. [123245111-33202R00111, ...])
    mode : str
        calling | reports, used to add context to error message
    single_dir : str (optional)
        single output directory, used to check for bam files where given
        sample doesn't appear in manifest to ensure its not a typo for
        launching reports

    Raises
    -------
    RuntimeError
        Raised when one or more exclude_samples not present in sample list
    """
    print("Checking provided exclude sample names are valid...")
    print("Samples specified to exclude:")
    prettier_print(exclude)

    # check that provided exclude names/patterns match to at least one
    exclude_not_present = [
        name for name in exclude
        if not any([re.match(name, sample) for sample in samples])
        and not name == r'^\w+-\w+Q\w+-'
    ]

    if exclude_not_present:
        if mode == 'reports' and single_dir:
            # one or more exclude samples not in manifest samples passed,
            # check the single output directory for BAM files, if found
            # count these as valid and drop from the exclude_not_present
            print(
                "The following samples were not present in the list of valid "
                f"sample names: {exclude_not_present}\n\nChecking the single "
                f"output directory ({single_dir}) for BAM files to be able "
                "to exclude"
            )
            sample_with_bam = []

            # ensure if single dir being specified with a project we use it
            project = None
            if single_dir.startswith('project-'):
                project, single_dir = single_dir.split(':', 1)

            for sample in exclude_not_present:
                print(
                    f"\nChecking {sample} for BAM file in project {project} "
                    f"and folder {single_dir}"
                )
                bam = list(dxpy.find_data_objects(
                    name=f"^{sample}.*.bam$",
                    name_mode='regexp',
                    project=project,
                    folder=single_dir,
                    limit=1,
                    describe=True
                ))

                if bam:
                    print(f"Found bam file: {bam[0]['describe']['name']}")
                    sample_with_bam.append(sample)
                else:
                    print(f"No bam file found for {sample}")

            exclude_not_present = list(
                set(exclude_not_present) - set(sample_with_bam))

            if not exclude_not_present:
                print(
                    "All samples specified to exclude have BAM file present "
                    "and therefore valid to be excluded from running reports"
                )
                return

        # provide some more info in logs for debugging
        print(
            f"Samples provided to exclude: {exclude}"
        )
        if mode == "calling":
            print(f"BAM files found to use for CNV calling: {samples}")
        else:
            print(f"Samples parsed from manifest: {samples}")

        print(
            "Samples specified to exclude that do not appear to be valid: "
            f"{exclude_not_present}"
        )

        raise RuntimeError(
            f"samples provided to exclude from CNV {mode} "
            f"not valid: {exclude_not_present}"
        )
    else:
        print("All exclude sample names valid")
