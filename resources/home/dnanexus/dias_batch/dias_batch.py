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
    from dias_batch.utils.utils import parse_manifest, split_manifest_tests, \
        split_genepanels_test_codes, check_manifest_valid_test_codes, \
        add_panels_and_indications_to_manifest, fill_config_reference_inputs, \
        make_path, prettier_print, time_stamp, write_summary_report
else:
    from .utils.dx_requests import DXExecute, DXManage
    from .utils.utils import parse_manifest, split_manifest_tests, \
        split_genepanels_test_codes, check_manifest_valid_test_codes, \
        add_panels_and_indications_to_manifest, fill_config_reference_inputs, \
        make_path, prettier_print, time_stamp, write_summary_report

import dxpy
import pandas as pd


class CheckInputs():
    """
    Basic methods for validating app inputs

    Raises
    ------
    RuntimeError
        Raised if one or more inputs is invalid
    """
    def __init__(self, **inputs) -> None:
        print("Validating inputs...")
        self.inputs = inputs
        self.errors = []
        self.check_assay()
        self.check_assay_config_dir()
        self.check_mode_set()
        self.check_single_output_dir()

        if self.errors:
            errors = '\n\t'.join(x for x in self.errors)
            raise RuntimeError(
                f"Errors in inputs passed:\n\t{errors}"
            )
        else:
            print("Inputs valid, continuing...")

    def check_assay(self):
        """Check assay string passed is valid"""
        if self.inputs['assay'] not in ['CEN', 'FH', 'TSOE', 'WES']:
            self.errors.append(
                f"Invalid assay passed: {self.inputs['assay']}"
            )

    def check_assay_config_dir(self):
        """Check that assay config dir is not empty"""
        if not self.inputs.get('assay_config_dir'):
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

        if self.inputs['single_output_dir'].startswith('project'):
            project, path = self.inputs['single_output_dir'].split(':')
        else:
            project = os.environ.get("DX_PROJECT_CONTEXT_ID")
            path = self.inputs['single_output_dir']

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

        if any(modes.pop(0)) and not self.inputs.get('manifest_file'):
            self.errors.append(
                'Reports argument specified with no manifest file'
            )

    def check_cnv_calling_for_cnv_reports(self):
        """
        Check if running CNV reports that either a job ID is given
        or running CNV calling at the same time
        """
        if self.inputs.get('cnv_reports') and not (
            self.inputs.get('cnv_call') or self.inputs.get('cnv_call_job_id')
        ):
            self.errors.append(
                "Running CNV reports without first running CNV calling and "
                "cnv_call_job_ID not specified. Please rerun with "
                "'-icnv_call=true or specify a job ID with '-icnv_call_job_id'"
            )


@dxpy.entry_point('main')
def main(
    assay=None,
    assay_config_file=None,
    assay_config_dir=None,
    manifest_file=None,
    split_tests=False,
    exclude_samples=None,
    single_output_dir=None,
    cnv_call_job_id=None,
    cnv_call=False,
    cnv_reports=False,
    snv_reports=False,
    mosaic_reports=False,
    testing=False,
    sample_limit=None
):
    check = CheckInputs(
        assay=assay,
        assay_config_file=assay_config_file,
        assay_config_dir=assay_config_dir,
        manifest_file=manifest_file,
        single_output_dir=single_output_dir,
        cnv_call=cnv_call,
        cnv_reports=cnv_reports,
        snv_reports=snv_reports,
        mosaic_reports=mosaic_reports
    )

    # assign single out dir in case of missing /output prefix to path
    single_output_dir = check.inputs['single_output_dir']

    # time of running for naming output folders
    start_time = time_stamp()

    dxpy.set_workspace_id(os.environ.get('DX_PROJECT_CONTEXT_ID'))

    assay_config = DXManage().get_assay_config(
        path=assay_config_dir,
        file=assay_config_file,
        assay=assay
    )

    assay_config = fill_config_reference_inputs(assay_config)

    if exclude_samples:
        exclude_samples = exclude_samples.split(',')

    # parse and format genepanels file
    genepanels = DXManage().read_dxfile(
        file=assay_config.get('reference_files', {}).get('genepanels'),
    )
    genepanels = pd.DataFrame(
        [x.split('\t') for x in genepanels],
        columns=['gemini_name', 'panel_name', 'hgnc_id']
    )
    genepanels.drop(columns=['hgnc_id'], inplace=True)  # chuck away HGNC ID
    genepanels = genepanels[genepanels.duplicated()]
    genepanels = split_genepanels_test_codes(genepanels)

    # parse manifest and format into a mapping of sampleID -> test codes
    manifest_data = DXManage().read_dxfile(manifest_file)
    manifest, manifest_source = parse_manifest(manifest_data)
    if split_tests:
        manifest = split_manifest_tests(manifest)

    # filter manifest tests against genepanels to ensure what has been
    # requested are test codes or HGNC IDs we recognise
    manifest, invalid_tests = check_manifest_valid_test_codes(
        manifest=manifest,
        genepanels=genepanels
    )

    # add in panel and clinical indication strings to manifest dict
    manifest = add_panels_and_indications_to_manifest(
        manifest=manifest,
        genepanels=genepanels
    )

    launched_jobs = {}
    cnv_reports_errors = snv_reports_errors = mosaic_reports_errors = None

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
                wait=wait
            )

            launched_jobs['CNV calling'] = [cnv_call_job_id]

    if cnv_reports:
        cnv_report_jobs, cnv_reports_errors, cnv_report_summary = \
            DXExecute().cnv_reports(
                workflow_id=assay_config.get('cnv_report_workflow_id'),
                call_job_id=cnv_call_job_id,
                single_output_dir=single_output_dir,
                manifest=manifest,
                manifest_source=manifest_source,
                config=assay_config['modes']['cnv_reports'],
                start=start_time,
                sample_limit=sample_limit
            )

        launched_jobs['cnv_reports'] = cnv_report_jobs

    if snv_reports:
        snv_reports, snv_reports_errors = DXExecute().snv_reports(
            workflow_id=assay_config.get('snv_report_workflow_id'),
            single_output_dir=single_output_dir,
            manifest=manifest,
            manifest_source=manifest_source,
            config=assay_config['modes']['snv_reports'],
            mosaic=False,
            start=start_time,
            sample_limit=sample_limit
        )
        launched_jobs['snv_reports'] = snv_reports

    if mosaic_reports:
        mosaic_reports, mosaic_reports_errors = DXExecute().snv_reports(
            workflow_id=assay_config.get('snv_report_workflow_id'),
            single_output_dir=single_output_dir,
            manifest=manifest,
            manifest_source=manifest_source,
            config=assay_config['modes']['mosaic_reports'],
            mosaic=True,
            start=start_time,
            sample_limit=sample_limit
        )
        launched_jobs['mosaic_reports'] = mosaic_reports

    print(
        'All jobs launched:\n\t',
        "\n\t".join([f"{x[0]}: {len(x[1])}" for x in launched_jobs.items()])
    )

    if testing and launched_jobs:
        # testing => terminate launched jobs
        print("Terminating launched jobs...")
        DXExecute().terminate(list(chain(*launched_jobs.values())))


    if any([
        invalid_tests, snv_reports_errors,
        cnv_reports_errors, mosaic_reports_errors
    ]):
        # some kind of error occured
        print("WARNING: one or more errors occured during launching of jobs:")
        if invalid_tests:
            print(
                "The following test codes provided were invalid "
                "and were excluded from analysis:"
            )
            prettier_print(invalid_tests)

        if snv_reports_errors:
            print("Errors launching SNV reports:")
            prettier_print(snv_reports_errors)

        if cnv_reports_errors:
            print("Errors launching CNV reports:")
            prettier_print(cnv_reports_errors)

        if mosaic_reports_errors:
            print("Errors launching mosaic reports:")
            prettier_print(mosaic_reports_errors)

    project_name = dxpy.describe(os.environ.get('DX_PROJECT_CONTEXT_ID'))['name']
    summary_file = f"{project_name}_{start_time}_job_summary.txt"

    write_summary_report(
        summary_file,
        assay_config=assay_config,
        manifest=manifest,
        launched_jobs=launched_jobs,
        invalid_tests=invalid_tests,
        snv_reports_errors=snv_reports_errors,
        cnv_report_errors=cnv_reports_errors,
        mosaic_reports_errors=mosaic_reports_errors,
        cnv_report_summary=cnv_report_summary
    )

    url_file = dxpy.upload_local_file(
        summary_file,
        folder=dxpy.bindings.dxjob.DXJob(
            os.environ.get('DX_JOB_ID')).describe()['folder']
    )

    return {"summary_report": dxpy.dxlink(url_file)}

dxpy.run()
