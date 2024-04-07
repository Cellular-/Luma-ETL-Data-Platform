###############################################################################
# Wrapper for developing luma-datawarehouse-datalake locally.
#
# .\workflow.ps1 <command> [<secondary>]
#
# - jrgarrar
###############################################################################

# Usage #######################################################################
### Common Commands ###########################################################
###    load_s3 <business_class>     
###                 Pulls data down from Infor and loads it into S3.
###
###    load_staging <business_class>     
###                 Pulls data down from S3 and loads it into Staging.
###
###    load_pipeline <business_class>     
###                 Pulls data down from Infor, loads it into S3, then loads it
###                 into Staging.
###
###    load_pipeline_subject_area <subject area>     
###                 Pulls data down from Infor, loads it into S3, then loads it
###                 into Staging. Runs for every business class in the target
###                 subject area.

###############################################################################
# SETUP #######################################################################
###############################################################################

# Parse arguments #############################################################
[CmdletBinding()]
param(
    [Parameter(Mandatory=$True)]
    [String]$command,
    [String]$secondary
)

# Global Variables ############################################################
$TENANT = "PRD"
$LOG_DIR = "$(Get-Location)/logs"
$RESOURCES_DIR = "$(Get-Location)/resources"
$TABLE_CONFIG_FILE = "$($RESOURCES_DIR)/table_configuration_mappings.json"
$SUBJECT_AREA_CONFIG_FILE = "$($RESOURCES_DIR)/subject_area_configuration_mappings.json"

# Environment Variables #######################################################
$ErrorActionPreference = "Stop"

###############################################################################
# HELPER FUNCTIONS ############################################################
###############################################################################

### Pull Data #################################################################
function schema_generation {
    venv/Scripts/python.exe -m datalakewrapper --gs 2>&1 | ForEach-Object{ "$_" }
}

function full_data_load {
    venv/Scripts/python.exe -m datalakewrapper --ed --fl 2>&1 | ForEach-Object{ "$_" }
}

function incremental_data_load {
    venv/Scripts/python.exe -m datalakewrapper --ed --il 2>&1 | ForEach-Object{ "$_" }
}

function data_compilation {
    param (
        $target_business_class
    )

    venv/Scripts/python.exe -m utilities.helpers.compile_data $target_business_class 2>&1 | ForEach-Object{ "$_" }
}

function pull_from_s3 {
    param (
        $target_s3_filename
    )

    venv/Scripts/python.exe -m utilities.helpers.fetch_s3_data "$($target_s3_filename)" 2>&1 | ForEach-Object{ "$_" }
}

function fetch_aux_files {
    param (
        $secondary
    )

    $file_list = (
        "$($secondary)_schemas.json",
        "$($secondary)_metadata.json" 
    )

    foreach ($filename in $file_list) {
        pull_from_s3 "$($filename)"
        Move-Item "landing-zone/$($filename)" "business-classes/$($TENANT)/" -Force 
    }
}

### Push Data#################################################################
function push_to_s3 {
    param (
        $target_business_class
    )

    # Parse config file
    $data_payload_json = Get-Content "business-classes\$($TENANT)\$($target_business_class)_push_data_payload.json" | ConvertFrom-Json

    # $file_list = (
    #     "$($target_business_class)_all_schemas.csv",
    #     "$($target_business_class)_metadata.json",
    #     "$($target_business_class)_schemas.json"
    # )

    # Push by file
    venv/Scripts/python.exe -m utilities.helpers.push_data "$($data_payload_json.all_schemas)" $TENANT --s3_path="$($data_payload_json.all_schemas_s3)" --bucket="databrew" --skip_databrew 2>&1 | ForEach-Object{ "$_" }
    venv/Scripts/python.exe -m utilities.helpers.push_data "$($data_payload_json.metadata)" $TENANT --s3_path="$($data_payload_json.metadata_s3)" --bucket="databrew" --skip_databrew 2>&1 | ForEach-Object{ "$_" }
    venv/Scripts/python.exe -m utilities.helpers.push_data "$($data_payload_json.schemas)" $TENANT --s3_path="$($data_payload_json.schemas_s3)" --bucket="databrew" --skip_databrew 2>&1 | ForEach-Object{ "$_" }
    venv/Scripts/python.exe -m utilities.helpers.push_data "$($data_payload_json.extraction_history)" $TENANT --s3_path="$($data_payload_json.extraction_history_s3)" --bucket="databrew" --skip_databrew 2>&1 | ForEach-Object{ "$_" }
    venv/Scripts/python.exe -m utilities.helpers.push_data "$($data_payload_json.db_load_payload)" $TENANT --s3_path="$($data_payload_json.db_load_payload_s3)" --bucket="databrew" --skip_databrew 2>&1 | ForEach-Object{ "$_" }
}

function push_etl_configs {
    venv/Scripts/python.exe -m utilities.helpers.push_etl_configs 2>&1 | ForEach-Object{ "$_" }
}

function push_to_staging {
    param (
        $s3_bucket,
        $target_file,
        $target_table,
        $mode
    )

    venv/Scripts/python.exe lambda_function.py "$($s3_bucket)" "$($target_file)" "$($target_table)" "$($mode)" 2>&1 | ForEach-Object{ "$_" }
}

### Databases #################################################################
function generate_table {
    venv/Scripts/python.exe -m metadata.createtablefrommetadata 2>&1 | ForEach-Object{ "$_" }
}

function delete_table {
    venv/Scripts/python.exe -m metadata.deletetable 2>&1 | ForEach-Object{ "$_" }
}

### Misc. ####################################################################
function fetch_target_business_class {
    return venv/Scripts/python.exe -m utilities.helpers.fetch_active_business_class
}

function fetch_aux_files {
    param (
        $secondary
    )
    try {
        pull_from_s3 "$($secondary)_extraction_history.csv" 
        Move-Item "landing-zone/$($secondary)_extraction_history.csv" "business-classes/$($TENANT)/" -Force 
        pull_from_s3 "$($secondary)_schemas.json" 
        Move-Item "landing-zone/$($secondary)_schemas.json" "business-classes/$($TENANT)/" -Force 
        pull_from_s3 "$($secondary)_metadata.json" 
        Move-Item "landing-zone/$($secondary)_metadata.json" "business-classes/$($TENANT)/" -Force 
        pull_from_s3 "$($secondary)_db_tbl_load_payload.json" 
        Move-Item "landing-zone/$($secondary)_db_tbl_load_payload.json" "business-classes/$($TENANT)/" -Force 
        return $true
    }
    catch {
        return $false
    }
}

function write_payload {
    param (
        $bc
    )

    venv/Scripts/python.exe -m utilities.helpers.write_db_load_payload --business_class=$bc
    venv/Scripts/python.exe -m utilities.helpers.write_push_data_payload --business_class=$bc
}

function change_target_business_class {
    param (
        $new_active_business_class
    )

    venv/Scripts/python.exe -m utilities.helpers.change_active_business_class $new_active_business_class
}

function summarize_daily_logs {
    $start_time = Get-Date 
    $start_time_formatted = $start_time.ToString("yyyy_MM_dd")
    venv/Scripts/python.exe -m utilities.helpers.log_scanner $start_time_formatted
}

function report_job {
    param (
        $business_class,
        $was_successful,
        $start_time
    )

    $start_time_formatted = $start_time.ToString("yyyy-MM-dd HH:mm:ss")
    $end_time = Get-Date
    $duration = $end_time - $start_time
    venv/Scripts/python.exe -m utilities.helpers.report_job $business_class $start_time_formatted $was_successful $duration
}

function report_duration {
    param (
        $start_time,
        $log_file_name
    )

    # Report the duration
    $end_time = Get-Date
    $duration = $end_time - $start_time
    Write-Output "LOAD DURATION: $($duration.ToString())" *>&1 | Tee-Object -FilePath "$($log_file_name)" -Append
}

function clean_business_class_files {
    ### Jump into the business-classes dir
    Push-Location "business-classes"

    ### For each directory within, nuke the generated files
    $dirs = Get-ChildItem -Directory
    foreach ($dir in $dirs) {
        Push-Location $dir
        Get-ChildItem -File | ForEach-Object { Remove-Item -Path $_.FullName }
        Get-ChildItem -Directory | ForEach-Object { Remove-Item -Path $_.FullName -Recurse }
        Pop-Location
    }

    Pop-Location
}

function clean_logs {
    ### Jump into the log dir
    Push-Location $LOG_DIR

    ### Nuke the log files
    Get-ChildItem *.log | ForEach-Object { Remove-Item -Path $_.FullName }

    Pop-Location
}

function push_incremental_structure {
    param (
        $target_business_class
    )

    venv/Scripts/python.exe -m utilities.helpers.create_inc_folders_s3 $target_business_class 2>&1 | ForEach-Object{ "$_" }
}

function update_job_tracker {
    venv/Scripts/python.exe -m utilities.helpers.run_stored_proc "maintenance_subject_area_proc" 2>&1 | ForEach-Object{ "$_" }
}

###############################################################################
# PRIMARY ETL FUNCTIONS #######################################################
###############################################################################
function load_s3 {
    param (
        $business_class_name
    )
    # Clean any leftover files
    clean_business_class_files 

    # Load settings
    $table_config_json = Get-Content $TABLE_CONFIG_FILE | ConvertFrom-Json
    $incremental_bool = $table_config_json.$business_class_name.incremental

    # Change the config file to use the target business class
    change_target_business_class $business_class_name 
    $target_business_class = fetch_target_business_class

    # Pull data from Infor
    if ( ($incremental_bool -eq $true) ) {
        fetch_aux_files $target_business_class
        incremental_data_load 
        push_incremental_structure 
    }
    else {
        full_data_load 
    }
    data_compilation -target_business_class $target_business_class 

    # Push data to S3
    write_payload $target_business_class 
    push_to_s3 $target_business_class 
}

function load_staging {
    param (
        $business_class_name
    )
    # Change the config file to use the target business class
    change_target_business_class $business_class_name 
    $target_business_class = fetch_target_business_class

    # Fetch metadata and other auxillary files
    fetch_aux_files $target_business_class

    # Drop the matching table in Staging and recreate it with the latest metadata
    delete_table 
    generate_table 

    # Push data to staging
    $table_config_json = Get-Content $TABLE_CONFIG_FILE | ConvertFrom-Json
    $load_parameters = Get-Content "business-classes/$($TENANT)/$($table_config_json.$business_class_name.business_class_name)_db_tbl_load_payload.json" | ConvertFrom-Json
    push_to_staging "$($load_parameters.s3_bucket)" "$($load_parameters.target_file)" "$($load_parameters.target_table)" "$($load_parameters.mode)"
}

function load_data_warehouse {
    param (
        $subject_area_name
    )

    # Run the loading script
    venv/Scripts/python.exe -m utilities.helpers.run_stored_proc "$($subject_area_name)_proc" 2>&1 | ForEach-Object{ "$_" }
}

function run_stored_proc {
    param (
        $stored_proc_name
    )

    # Run the stored procedure
    venv/Scripts/python.exe -m utilities.helpers.run_stored_proc "$($stored_proc_name)" 2>&1 | ForEach-Object{ "$_" }
}

###############################################################################
# COMMANDS ####################################################################
###############################################################################

### Infor -> S3 ###############################################################
if ($command -eq "load_s3") {
    # Mark down the start time
    $start_time = Get-Date 
    $start_time_formatted = $start_time.ToString("yyyy_MM_dd_HH_mm")
    $log_file_name = "$($LOG_DIR)/$($start_time_formatted)_$($secondary)_load_s3.log"

    try {
        # Run the Infor -> S3 script
        load_s3 $secondary *>&1 | Tee-Object -FilePath "$($log_file_name)"
    }
    catch {
        Write-Output "Infor Data Load Failed: $($secondary)" | Tee-Object -FilePath "$($log_file_name)" -Append
        Write-Output "$($_)" | Tee-Object -FilePath "$($log_file_name)" -Append
        report_duration $start_time $log_file_name
        throw
    }

    report_duration $start_time $log_file_name
}

### S3 -> Staging Table #######################################################
elseif ($command -eq "load_staging") {
    # Mark down the start time
    $start_time = Get-Date 
    $start_time_formatted = $start_time.ToString("yyyy_MM_dd_HH_mm")
    $log_file_name = "$($LOG_DIR)/$($start_time_formatted)_$($secondary)_load_staging.log"

    try {
        # Run the S3 -> Staging script
        load_staging $secondary *>&1 | Tee-Object -FilePath "$($log_file_name)"
    }
    catch {
        # Record errors and break
        Write-Output "Load to Staging Failed: $($secondary)" | Tee-Object -FilePath "$($log_file_name)" -Append
        Write-Output "$($_)" | Tee-Object -FilePath "$($log_file_name)" -Append

        # Report the duration
        report_duration $start_time $log_file_name
        throw
    }

    report_duration $start_time $log_file_name
}

### Staging Table -> Data Warehouse Table #####################################
elseif ($command -eq "load_data_warehouse") {
    # Mark down the start time
    $start_time = Get-Date 
    $start_time_formatted = $start_time.ToString("yyyy_MM_dd_HH_mm")
    $log_file_name = "$($LOG_DIR)/$($start_time_formatted)_$($secondary)_load_data_warehouse.log"

    try {
        # Run the S3 -> Staging script
        load_data_warehouse $secondary *>&1 | Tee-Object -FilePath "$($log_file_name)"
    }
    catch {
        # Record errors and break
        Write-Output "Load to Data Warehouse Failed: $($secondary)" | Tee-Object -FilePath "$($log_file_name)" -Append
        Write-Output "$($_)" | Tee-Object -FilePath "$($log_file_name)" -Append

        # Report the duration
        report_duration $start_time $log_file_name
        throw
    }

    report_duration $start_time $log_file_name
}

### Infor -> S3 -> Staging Table ##############################################
elseif ($command -eq "load_pipeline") {
    # Mark down the start time
    $start_time = Get-Date 
    $start_time_formatted = $start_time.ToString("yyyy_MM_dd_HH_mm")
    $log_file_name = "$($LOG_DIR)/$($start_time_formatted)_$($secondary)_load_pipeline.log"

    try {
        # Run the Infor -> S3 script
        load_s3 $secondary *>&1 | Tee-Object -FilePath "$($log_file_name)"

        # Run the S3 -> Staging script
        load_staging $secondary *>&1 | Tee-Object -FilePath "$($log_file_name)" -Append

        # Run the Staging -> Data Warehouse script
        load_data_warehouse $secondary *>&1 | Tee-Object -FilePath "$($log_file_name)" -Append
    }
    catch {
        # Record errors and break
        Write-Output "Load Failed: $($secondary)" | Tee-Object -FilePath "$($log_file_name)" -Append
        Write-Output "$($_)" | Tee-Object -FilePath "$($log_file_name)" -Append

        # Report 
        $target_business_class = fetch_target_business_class
        report_job $target_business_class "false" $start_time 
        report_duration $start_time $log_file_name
        throw
    }

    $target_business_class = fetch_target_business_class
    report_job $target_business_class "false" $start_time 
    report_duration $start_time $log_file_name
}

### Infor -> S3 #######################################################
elseif ($command -eq "load_s3_subject_area") {
    # Mark down the start time
    $start_time = Get-Date 
    $start_time_formatted = $start_time.ToString("yyyy_MM_dd_HH_mm")
    $log_file_name = "$($LOG_DIR)/$($start_time_formatted)_$($secondary)_load_pipeline.log"

    # Load in aliases for each subject area
    $subject_area_json = Get-Content $SUBJECT_AREA_CONFIG_FILE | ConvertFrom-Json

    # Pull a list of target business classes from the aliases
    $target_business_classes = $subject_area_json.$secondary

    # Iterate over the business classes, pulling them down one-by-one
    foreach ($bc in $target_business_classes) {
        Write-Output "Pulling $($bc)..."
        try {
            $sub_start_time = Get-Date 

            # Run the Infor -> S3 script
            load_s3 $bc *>&1 | Tee-Object -FilePath "$($log_file_name)" -Append
        }
        catch {
            # Record errors, then continue to the next business class
            Write-Output "Load Failed: $($bc)" | Tee-Object -FilePath "$($log_file_name)" -Append
            Write-Output "$($_)" | Tee-Object -FilePath "$($log_file_name)" -Append
            $target_business_class = fetch_target_business_class
        }
    }

    report_duration $start_time $log_file_name
}

### S3 -> Data Warehouse #######################################################
elseif ($command -eq "load_pipeline_subject_area") {
    # Mark down the start time
    $start_time = Get-Date 
    $start_time_formatted = $start_time.ToString("yyyy_MM_dd_HH_mm")
    $log_file_name = "$($LOG_DIR)/$($start_time_formatted)_$($secondary)_load_pipeline.log"

    # Load in aliases for each subject area
    $subject_area_json = Get-Content $SUBJECT_AREA_CONFIG_FILE | ConvertFrom-Json

    # Pull a list of target business classes from the aliases
    $target_business_classes = $subject_area_json.$secondary

    # Iterate over the business classes, pulling them down one-by-one
    foreach ($bc in $target_business_classes) {
        Write-Output "Pulling $($bc)..."
        try {
            $sub_start_time = Get-Date 

            # Run the Infor -> S3 script
            load_s3 $bc *>&1 | Tee-Object -FilePath "$($log_file_name)" -Append

            # Run the S3 -> Staging script
            load_staging $bc *>&1 | Tee-Object -FilePath "$($log_file_name)" -Append

            # Run the Staging -> Data Warehouse script
            load_data_warehouse $bc *>&1 | Tee-Object -FilePath "$($log_file_name)" -Append

            $target_business_class = fetch_target_business_class
            report_job $target_business_class "true" $sub_start_time 
        }
        catch {
            # Record errors, then continue to the next business class
            Write-Output "Load Failed: $($bc)" | Tee-Object -FilePath "$($log_file_name)" -Append
            Write-Output "$($_)" | Tee-Object -FilePath "$($log_file_name)" -Append
            $target_business_class = fetch_target_business_class
            report_job $target_business_class "false" $sub_start_time 
        }
    }

    report_duration $start_time $log_file_name
}

elseif ($command -eq "clean") {
    clean_logs
}

### Commands: Utility #########################################################
elseif ($command -eq "pull_output_file") {
    pull_from_s3 $secondary
}

elseif ($command -eq "summarize_daily_logs") {
    summarize_daily_logs
}

elseif ($command -eq "update_job_tracker") {
    update_job_tracker
}

elseif ($command -eq "push_etl_configs") {
    push_etl_configs
}

elseif ($command -eq "run_stored_proc") {
    run_stored_proc $secondary
}

elseif ($command -eq "fetch_aux_files") {
    fetch_aux_files $secondary
}

elseif ($command -eq "write_payload") {
    write_payload $secondary
}

elseif ($command -eq "push_to_s3") {
    push_to_s3 $secondary
}

### Commands: Not Recognized ##################################################
else {
    Write-Output "Command not recognized."
}
