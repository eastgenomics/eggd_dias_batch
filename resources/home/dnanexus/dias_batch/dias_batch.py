from pathlib import Path
import os
import sys

# Install required packages
for package in os.listdir("/home/dnanexus/packages"):
    print(f"Installing {package}")
    pip.main(["install", "--no-index", "--no-deps", f"packages/{package}"])


import dxpy

from .utils.dx_requests import DXExecute, DXManage



@dxpy.entry_point('main')
def main(
    assay,
    assay_config_file,
    assay_config_dir,
    manifest_file,
    single_output_dir,
    cnv_call,
    cnv_report,
    snv_report,
    mosaic_report,
    testing
):
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
            wait=wait
        )



dxpy.run()