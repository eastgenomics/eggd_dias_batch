import datetime
import subprocess

def get_date():
    date = datetime.datetime.now()
    return date.strftime("%y%m%d")

def dx_make_workflow_dir(dx_dir_path):
    command = "dx mkdir -p /output/; dx mkdir {dx_dir_path}".format(dx_dir_path=dx_dir_path)
    try:
        subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True, )
        return True
    except subprocess.CalledProcessError as cmdexc:
        #print("Status : FAIL", cmdexc.returncode, cmdexc.output)
        return False

def describe_workflow(workflow_id):
    command="dx describe {workflow_id}".format(workflow_id=workflow_id)
    
    # This try except is used to handle permission errors generated when dx describe tries 
    # to get info about files we do not have permission to access. 
    # In these cases the description is returned but the commands has non-0 exit status so errors out
    
    try:
        workflow_description = subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)
    except subprocess.CalledProcessError as cmdexc:
        workflow_description = str(cmdexc.output)
    return workflow_description.split("\n")

def get_object_name_from_object_id(workflow_id):
    workflow_description = describe_workflow(workflow_id)

    for line in workflow_description:
        if line.startswith("Name "):
            workflow_name = line.split(" ")[-1]
            return workflow_name
    return None

def get_app_name_from_app_id(app_id):
    pass


def make_workflow_out_dir(workflow_id):
    workflow_name = get_object_name_from_object_id(workflow_id)
    assert workflow_name, "Workflow name not found. Aborting"
    workflow_output_dir_pattern = "/output/{workflow_name}-{date}-{index}/"
    
    i = 1
    while i < 100: # If we have more than 100 instances of a workflow something is very wrong!
        workflow_output_dir = workflow_output_dir_pattern.format(workflow_name=workflow_name, date=get_date(), index=i)
        if dx_make_workflow_dir(workflow_output_dir):
            print("Using\t\t%s" % workflow_output_dir)
            return workflow_output_dir
        else:
            print("Skipping\t%s" % workflow_output_dir)
        i += 1
    return None

def get_workflow_stage_info(workflow_id):
    workflow_description = describe_workflow(workflow_id)

    stages={}
    
    previous_line_is_stage=False
    for index, line in enumerate(workflow_description):
        
        if line.startswith("Stage "):
            previous_line_is_stage=True
            stage = line.split(" ")[1]
        
        # If prev line was stage line then this line contains executable
        elif previous_line_is_stage:
            assert line.startswith("  Executable"), "Expected '  Executable' line after stage line {line_num}\n{line}".format(line_num=index+1, line=line)
            app_id = line.split(" ")[-1]
            app_name = get_object_name_from_object_id(app_id)
            
            stages[stage] = {"app_id":app_id,
                             "app_name":app_name}
            previous_line_is_stage=False
        
        else:
            previous_line_is_stage=False

    return stages


def make_app_out_dirs(workflow_stage_info, workflow_id, workflow_output_dir):
    for stage, stage_info in sorted(workflow_stage_info.items()):
        app_out_dir = "{workflow_output_dir}{app_name}".format(workflow_output_dir=workflow_output_dir, app_name=stage_info["app_name"])
        command = "dx mkdir {app_out_dir}".format(app_out_dir=app_out_dir)
        subprocess.check_output(command, shell=True)
    return True


workflow_id         = "workflow-FpG0K7Q4yjFkkf05P6xJqZbq"
workflow_out_dir    = make_workflow_out_dir(workflow_id)
workflow_stage_info = get_workflow_stage_info(workflow_id)
app_out_dirs        = make_app_out_dirs(workflow_stage_info, workflow_id, workflow_out_dir)
