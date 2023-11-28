"""Main entry point script for the app"""
from glob import glob
from itertools import chain
import os
import re
import subprocess

if os.path.exists('/home/dnanexus'):
    # running in DNAnexus
    subprocess.check_call([
        'pip', 'install', "--no-index", "--no-deps"
    ] + glob("packages/*"))

    from dias_batch.utils.dx_requests import DXExecute, DXManage
    from dias_batch.utils.utils import (
        add_panels_and_indications_to_manifest,
        check_manifest_valid_test_codes,
        fill_config_reference_inputs,
        make_path,
        parse_manifest,
        parse_genepanels,
        prettier_print,
        time_stamp,
        write_summary_report
    )
else:
    from .utils.dx_requests import DXExecute, DXManage
    from .utils.utils import (
        add_panels_and_indications_to_manifest,
        check_manifest_valid_test_codes,
        fill_config_reference_inputs,
        make_path,
        parse_manifest,
        parse_genepanels,
        prettier_print,
        time_stamp,
        write_summary_report
    )

import dxpy
import pandas as pd


# for prettier viewing in the logs
pd.set_option('display.max_rows', 200)
pd.set_option('max_colwidth', 1500)


class CheckInputs():
    """
    Basic methods for validating app inputs

    Raises
    ------
    RuntimeError
        Raised if one or more inputs is invalid
    """
    def __init__(self, **inputs) -> None:
        input_str = '\n\t'.join(f"{k} : {v}" for k, v in inputs.items())
        print(f"\n \nValidating inputs, inputs provided:\n\t{input_str}")

        self.inputs = inputs
        self.errors = []
        self.check_assay()
        self.check_assay_config_dir()
        self.check_mode_set()
        self.check_single_output_dir()
        self.check_cnv_call_and_cnv_call_job_id_mutually_exclusive()
        self.check_cnv_calling_for_cnv_reports()
        self.check_artemis_inputs()
        self.check_exclude_str_and_file()
        self.check_exclude_samples_file_id()

        if self.errors:
            errors = '; '.join(x for x in self.errors)
            raise RuntimeError(
                f"Errors in job inputs:\n\t{errors}"
            )

        print("Inputs valid, continuing...")

    def check_assay(self):
        """Check assay string passed is valid"""
        if self.inputs['assay'] not in ['CEN', 'TWE']:
            self.errors.append(
                f"Invalid assay passed: {self.inputs['assay']}"
            )

    def check_assay_config_dir(self):
        """Check that assay config dir is not empty"""
        if not self.inputs.get('assay_config_dir') or \
            self.inputs.get('assay_config_file'):
                return

        project, path = self.inputs['assay_config_dir'].split(':')
        files = list(dxpy.find_data_objects(
            name="*.json",
            name_mode='glob',
            project=project,
            folder=path,
            describe=True
        ))

        if not files:
            self.errors.append(
                "Given assay config dir appears to contain no config "
                f"files: {self.inputs['assay_config_dir']}"
            )

    def check_single_output_dir(self):
        """Check single output dir is not empty"""
        if not self.inputs.get('single_output_dir'):
            return

        if self.inputs['single_output_dir'].startswith('project-'):
            project, path = self.inputs['single_output_dir'].strip().split(':')
        else:
            project = os.environ.get("DX_PROJECT_CONTEXT_ID")
            path = self.inputs['single_output_dir'].strip()

        files = list(dxpy.find_data_objects(
            project=project,
            folder=path,
            limit=1
        ))

        if not files:
            # dir appears empty, try again if not prefixed with /output/
            if not re.match(r'/output', path):
                prefix_path = make_path('/output', path)
                files = list(dxpy.find_data_objects(
                    project=project,
                    folder=prefix_path,
                    limit=1
                ))
                if files:
                    print(
                        f"{path} returned no files but files found in "
                        f"{prefix_path}, will use this for analysis"
                    )
                    self.inputs['single_output_dir'] = prefix_path
                    return

            self.errors.append(
                "Given Dias single output dir appears to be empty: "
                f"{self.inputs['single_output_dir']}"
            )

    def check_mode_set(self):
        """Check at least one running mode set and manifest passed if running reports"""
        modes = ['cnv_call', 'cnv_reports', 'snv_reports', 'mosaic_reports']
        if not any(self.inputs.get(x) for x in modes):
            self.errors.append('No mode specified to run in')

        modes.pop(0)
        if any([
            self.inputs.get(x) for x in modes
        ]) and not self.inputs.get('manifest_files'):
            self.errors.append(
                'Reports argument specified with no manifest file'
            )

    def check_cnv_call_and_cnv_call_job_id_mutually_exclusive(self):
        """
        Check that both cnv_call and cnv_call_job_id have not been
        specified together
        """
        if self.inputs.get('cnv_call') and self.inputs.get('cnv_call_job_id'):
            self.errors.append(
                'Both mutually exclusive cnv_call and '
                'cnv_call_job_id inputs specified'
            )

    def check_cnv_calling_for_cnv_reports(self):
        """
        Check if running CNV reports that either a job ID is given
        or running CNV calling at the same time
        """
        if self.inputs.get('cnv_reports'):
            if (
                not self.inputs.get('cnv_call') and
                not self.inputs.get('cnv_call_job_id')
            ):
                self.errors.append(
                    "Running CNV reports without first running CNV calling and "
                    "cnv_call_job_ID not specified. Please rerun with "
                    "'-icnv_call=true or specify a job ID with '-icnv_call_job_id'"
                )

    def check_artemis_inputs(self):
        """Check if running artemis that the required inputs are set"""
        if self.inputs.get('artemis'):
            if (
                not self.inputs.get('cnv_reports') and
                not self.inputs.get('snv_reports')
            ):
                self.errors.append(
                    "Artemis specified to run but no snv or cnv reports "
                    "specified. Please rerun with -icnv_reports and / or "
                    "-isnv_reports"
                )

    def check_exclude_str_and_file(self):
        """
        Check when -iexclude_samples or -iexclude_samples_file is passed
        that only one is specified
        """
        if all([
            self.inputs.get('exclude_samples'),
            self.inputs.get('exclude_samples_file')
        ]):
            self.errors.append(
                "Both -iexclude_samples and -iexclude_samples_file specified, "
                "only one may be specified"
            )

    def check_exclude_samples_file_id(self):
        """
        Check if input to -iexclude_samples is a file-xxx string and should
        have been provided to -iexclude_samples_file
        """
        if self.inputs.get('exclude_samples'):
            if re.match(r"file-", self.inputs.get('exclude_samples')):
                self.errors.append(
                    "DNAnexus file ID provided to -iexclude_samples, "
                    "rerun and provide this as -iexclude_samples_file="
                    f"{self.inputs.get('exclude_samples')}"
                )


@dxpy.entry_point('main')
def main(
    assay=None,
    assay_config_file=None,
    assay_config_dir=None,
    manifest_files=None,
    split_tests=False,
    exclude_samples=None,
    exclude_samples_file=None,
    manifest_subset=None,
    single_output_dir=None,
    cnv_call_job_id=None,
    cnv_call=False,
    cnv_reports=False,
    snv_reports=False,
    mosaic_reports=False,
    artemis=False,
    qc_file=None,
    testing=False,
    sample_limit=None,
    unarchive=None
):
    dxpy.set_workspace_id(os.environ.get('DX_PROJECT_CONTEXT_ID'))

    check = CheckInputs(**locals())

    # assign single out dir in case of missing / output prefix to path
    single_output_dir = check.inputs['single_output_dir']

    # time of running for naming output folders
    start_time = time_stamp()

    if assay_config_file:
        assay_config = DXManage().read_assay_config_file(
            file=assay_config_file.get('$dnanexus_link')
        )
    elif assay and assay_config_dir:
        assay_config = DXManage().get_assay_config(
            assay=assay,
            path=assay_config_dir
        )
    else:
        raise RuntimeError(
            "No assay config file or assay and config dir provided"
        )

    assay_config = fill_config_reference_inputs(assay_config)

    if exclude_samples:
        exclude_samples = exclude_samples.split(',')

    if exclude_samples_file:
        exclude_samples = DXManage().read_dxfile(exclude_samples_file)

    # parse and format genepanels file
    genepanels_data = DXManage().read_dxfile(
        file=assay_config.get('reference_files', {}).get('genepanels'),
    )
    genepanels = parse_genepanels(genepanels_data)

    if manifest_files:
        # one or more manifest files specified => parse manifest(s)
        # and format into a mapping of sampleID -> test codes
        print(f"{len(manifest_files)} manifest file(s) passed")
        manifest = {}
        manifest_source = {}

        for file in manifest_files:
            manifest_data = DXManage().read_dxfile(file)
            manifest_data, source = parse_manifest(
                contents=manifest_data,
                split_tests=split_tests,
                subset=manifest_subset
            )

            # combine manifest data to previous
            manifest = {**manifest, **manifest_data}
            manifest_source = {**manifest_source, **source}

        print("Parsed manifest(s)")
        prettier_print(manifest)

        # filter manifest tests against genepanels to ensure what has been
        # requested are test codes or HGNC IDs we recognise
        manifest = check_manifest_valid_test_codes(
            manifest=manifest,
            genepanels=genepanels
        )

        # add in panel and clinical indication strings to manifest dict
        manifest = add_panels_and_indications_to_manifest(
            manifest=manifest,
            genepanels=genepanels
        )

        # combine manifest source for each sample into its manifest values
        manifest = {
            sample: {**manifest[sample], **manifest_source[sample]}
            for sample in manifest
        }

    launched_jobs = {}
    cnv_report_errors = snv_report_errors = mosaic_report_errors = \
        cnv_report_summary = snv_report_summary = mosaic_report_summary = None

    # set downstream jobs to be dependent on parent batch job, wonderfully
    # hacky way to not actually start any downstream jobs in testing mode and
    # having to clean up after jobs that managed to complete during launching
    if testing:
        parent = [os.environ.get("DX_JOB_ID")]
    else:
        parent = None

    if cnv_call:
        if cnv_call_job_id:
            print(
                "WARNING: both 'cnv_call' set and 'cnv_call_job_id' "
                "specified.\nWill use output of specified job "
                f"({cnv_call_job_id}) instead of running CNV calling."
            )
        else:
            # check if we're running reports after and to hold app
            # until CNV calling completes
            wait = True if cnv_reports else False

            cnv_call_job_id = DXExecute().cnv_calling(
                config=assay_config,
                single_output_dir=single_output_dir,
                exclude=exclude_samples,
                start=start_time,
                wait=wait,
                unarchive=unarchive
            )

            launched_jobs['CNV calling'] = [cnv_call_job_id]

    if cnv_reports:
        cnv_report_jobs, cnv_report_errors, cnv_report_summary = \
            DXExecute().reports_workflow(
                mode='CNV',
                workflow_id=assay_config.get('cnv_report_workflow_id'),
                single_output_dir=single_output_dir,
                manifest=manifest,
                config=assay_config['modes']['cnv_reports'],
                start=start_time,
                name_patterns=assay_config.get('name_patterns', {}),
                sample_limit=sample_limit,
                call_job_id=cnv_call_job_id,
                parent=parent,
                unarchive=unarchive,
                exclude=exclude_samples
            )

        launched_jobs['cnv_reports'] = cnv_report_jobs

    if snv_reports:
        snv_reports, snv_report_errors, snv_report_summary = \
            DXExecute().reports_workflow(
                mode='SNV',
                workflow_id=assay_config.get('snv_report_workflow_id'),
                single_output_dir=single_output_dir,
                manifest=manifest,
                config=assay_config['modes']['snv_reports'],
                start=start_time,
                name_patterns=assay_config.get('name_patterns', {}),
                sample_limit=sample_limit,
                parent=parent,
                unarchive=unarchive
            )
        launched_jobs['snv_reports'] = snv_reports

    if mosaic_reports:
        mosaic_reports, mosaic_report_errors, mosaic_report_summary = \
            DXExecute().reports_workflow(
                mode='mosaic',
                workflow_id=assay_config.get('snv_report_workflow_id'),
                single_output_dir=single_output_dir,
                manifest=manifest,
                config=assay_config['modes']['mosaic_reports'],
                start=start_time,
                name_patterns=assay_config.get('name_patterns', {}),
                sample_limit=sample_limit,
                parent=parent,
                unarchive=unarchive
            )
        launched_jobs['mosaic_reports'] = mosaic_reports

    if artemis:
        # get parent output path of all reports workflows
        snv_path = cnv_path = None

        if launched_jobs.get('snv_reports'):
            snv_path = dxpy.describe(
                launched_jobs.get('snv_reports')[0])['folder']

        if launched_jobs.get('cnv_reports'):
            cnv_path = dxpy.describe(
                launched_jobs.get('cnv_reports')[0])['folder']

        dependent_jobs = [
            job for job_list in launched_jobs.values() for job in job_list
        ]

        if snv_path or cnv_path:
            artemis_job = DXExecute().artemis(
                single_output_dir=single_output_dir,
                app_id=assay_config.get('artemis_app_id'),
                dependent_jobs=dependent_jobs,
                start=start_time,
                qc_xlsx=qc_file,
                snv_output=snv_path,
                cnv_output=cnv_path,
                capture_bed=assay_config['modes']['artemis']['inputs']['capture_bed']
            )

            launched_jobs['artemis'] = [artemis_job]
        else:
            print("No SNV or CNV reports launched to run Artemis for!")

    print(
        'All jobs launched:\n\t',
        "\n\t".join([f"{x[0]}: {len(x[1])}" for x in launched_jobs.items()])
    )

    if testing and launched_jobs:
        # testing => terminate launched jobs
        print("Terminating launched jobs...")
        DXExecute().terminate(list(chain(*launched_jobs.values())))

    project_name = dxpy.describe(os.environ.get('DX_PROJECT_CONTEXT_ID'))['name']
    summary_file = f"{project_name}_{start_time}_job_summary.txt"

    job_details = dxpy.DXJob(dxid=os.environ.get('DX_JOB_ID')).describe()
    app_details = dxpy.DXApp(dxid=job_details['executable']).describe()

    # overwrite manifest job ID in job details with name to write to summary
    if manifest_files:
        manifest_names = []
        for file in job_details['runInput']['manifest_files']:
            manifest_names.append(dxpy.describe(file['$dnanexus_link'])['name'])

        job_details['runInput']['manifest_files'] = ', '.join(manifest_names)

    write_summary_report(
        summary_file,
        job=job_details,
        app=app_details,
        assay_config=assay_config,
        manifest=manifest,
        launched_jobs=launched_jobs,
        excluded=exclude_samples,
        snv_report_errors=snv_report_errors,
        cnv_report_errors=cnv_report_errors,
        mosaic_report_errors=mosaic_report_errors,
        cnv_report_summary=cnv_report_summary,
        snv_report_summary=snv_report_summary,
        mosaic_report_summary=mosaic_report_summary
    )

    url_file = dxpy.upload_local_file(
        summary_file,
        folder=dxpy.bindings.dxjob.DXJob(
            os.environ.get('DX_JOB_ID')).describe()['folder']
    )

    launched_jobs = ','.join([
        job for job_list in launched_jobs.values() for job in job_list
    ])

    return {
        "summary_report": dxpy.dxlink(url_file),
        "launched_jobs": launched_jobs
    }

if os.path.exists('/home/dnanexus'):
    # check for env to allow importing CheckInputs for unit tests
    dxpy.run()
