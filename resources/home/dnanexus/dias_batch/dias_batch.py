from pathlib import Path
import os
import subprocess
import sys

# Install required packages
subprocess.check_call([
    sys.executable, '-m ', 'pip', 'install',
    "--no-index", "--no-deps", "packages/*"
])

import dxpy

subprocess.check_call(['ls'])

from utils.dx_requests import DXExecute, DXManage


class CheckInputs():
    """
    Basic methods for validating app inputs
    """
    def __init__(self, **inputs):
        self.inputs = inputs
        self.errors = []
    
    def check_assay(self):
        """Check assay string passed is valuid"""
        if self.inputs['assay'] not in ['CEN', 'WES']:
            self.errors.append(
                f"Invalid assay passed: {self.inputs['assay']}"
            )
    
    def check_assay_config_dir(self):
        """Check that assay config dir is not empty"""
        if not self.inputs.get('assay_config_dir'):
            return

        project, path = self.inputs['assay_config_dir'].split(':')
        files = list(dx.find_data_objects(
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

        files = list(dx.find_data_objects(
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
    assay,
    assay_config_file,
    assay_config_dir,
    manifest_file,
    exclude_samples,
    single_output_dir,
    cnv_call,
    cnv_report,
    snv_report,
    mosaic_report,
    testing
):
    print(locals())

    check = CheckInputs(assay,
        assay_config_file,
        assay_config_dir,
        manifest_file,
        single_output_dir,
        cnv_call,
        cnv_report,
        snv_report,
        mosaic_report
    )

    if check.errors:
        errors = '\n'.join(x for x in check.errors)
        raise RuntimeError(
            f"Errors in inputs passed: {errors}"
        )
    else:
        (
            assay, assay_config_file,
            assay_config_dir,
            manifest_file,
            single_output_dir,
            cnv_call,
            cnv_report,
            snv_report,
            mosaic_report
        ) = check.inputs.values()


    assay_config = DXManage.get_assay_config(
        path=assay_config_dir,
        file=assay_config_file,
        assay=assay
    )

    manifest_file = DXManage.read_manifest()
    
    if cnv_call:
        if any(cnv_report, snv_report, mosaic_report):
            # going to run some reports after calling finishes, hold app
            # until calling completes
            wait=True
        else:
            wait=False

        DXExecute.run_cnv_calling(
            config=assay_config,
            single_output_dir=single_output_dir,
            manifest=manifest,
            exclude=exclude_samples,
            wait=wait
        )
    
    if cnv_report:
        pass
    
    if snv_report:
        pass

    if mosaic_report:
        pass


dxpy.run()