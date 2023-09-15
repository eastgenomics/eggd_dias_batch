from glob import glob
from itertools import chain
from pathlib import Path
import os
import subprocess
import sys

if os.path.exists('/home/dnanexus'):
    # running in DNAnexus
    subprocess.check_call([
        'pip', 'install', "--no-index", "--no-deps"
    ] + glob("packages/*"))

    from dias_batch.utils.dx_requests import DXExecute, DXManage
    from dias_batch.utils.utils import parse_manifest, split_tests
else:
    from .utils.dx_requests import DXExecute, DXManage
    from .utils.utils import parse_manifest, split_tests

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
        print("Validating inputs")
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
            describe=True
        ))

        if not files:
            self.errors.append(
                "Given Dias single output dir appears to be empty: "
                f"{self.inputs['single_output_dir']}"
            )
    
    def check_mode_set(self):
        """Check at least one running mode set"""
        if not any(
            self.inputs.get(x) for x in 
            ['cnv_call', 'cnv_report', 'snv_report', 'mosaic_report']):
                self.errors.append('No mode specified to run in')


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
    cnv_report=False,
    snv_report=False,
    mosaic_report=False,
    testing=False
):
    check = CheckInputs(
        assay=assay,
        assay_config_file=assay_config_file,
        assay_config_dir=assay_config_dir,
        manifest_file=manifest_file,
        single_output_dir=single_output_dir,
        cnv_call=cnv_call,
        cnv_report=cnv_report,
        snv_report=snv_report,
        mosaic_report=mosaic_report
    )

    dxpy.set_workspace_id(os.environ.get('DX_PROJECT_CONTEXT_ID'))

    assay_config = DXManage().get_assay_config(
        path=assay_config_dir,
        file=assay_config_file,
        assay=assay
    )

    if exclude_samples:
        exclude_samples = exclude_samples.split(',')

    manifest_data = DXManage().read_dxfile(manifest_file)
    manifest = parse_manifest(manifest_data)
    if split_tests:
        manifest = split_tests(manifest)

    genepanels = DXManage().read_dxfile(
        file=assay_config.get('reference_files', {}).get('genepanels'),
    )
    genepanels = pd.DataFrame(
        [x.split('\t') for x in genepanels],
        columns=['gemini_name', 'panel_name', 'hgnc_id'],
        dtype='category'
    )

    launched_jobs = {}
    
    if cnv_call:
        if cnv_report:
            # going to run some reports after calling finishes,
            # hold app until calling completes
            wait=True
        else:
            wait=False

        job_id = DXExecute().cnv_calling(
            config=assay_config,
            single_output_dir=single_output_dir,
            exclude=exclude_samples,
            wait=wait
        )
        launched_jobs['CNV calling'] = [job_id]

    if cnv_report:
        pass
    
    if snv_report:
        pass

    if mosaic_report:
        pass
 
    print(
        f'All jobs launched:\n\t',
        "\n\t".join([f"{x[0]}: {x[1]}" for x in launched_jobs.items()])
    )

    if testing and launched_jobs:
        # testing => terminate launched jobs
        print("Terminating launched jobs")
        DXExecute().terminate(list(chain(*launched_jobs.values())))
   

dxpy.run()
