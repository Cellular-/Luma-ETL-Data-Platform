param (
    [String] $ScriptRoot=$PSScriptRoot
)

# Hop into the proper directory
Push-Location $ScriptRoot

# Setup a run log
$LOG_DIR = "$(Get-Location)/logs"
$start_time = Get-Date 
$start_time_formatted = $start_time.ToString("yyyy_MM_dd_HH_mm")
$log_file_name = "$($LOG_DIR)/$($start_time_formatted)_run.log"
Write-Output "$($(Get-Date).ToString("yyyy:MM:dd_HH:mm:ss")) - INFO - START" | Tee-Object -FilePath "$($log_file_name)"

# Iterate through subject areas
$targets = @(
    "billing",
    "budget",
    "cash",
    "custodial",
    "payables",
    "project",
    "purchasing",
    "receivables",
    "general_ledger",
    "human_resources",
    "workforce_management",
    "maintenance",
    "xm"
)
foreach ($target in $targets) {
    try {
        Write-Output "$($(Get-Date).ToString("yyyy:MM:dd_HH:mm:ss")) - INFO - Running $($target)..." | Tee-Object -FilePath "$($log_file_name)" -Append
        .\workflow.ps1 load_pipeline_subject_area $target
    }
    catch {
        Write-Output "$($(Get-Date).ToString("yyyy:MM:dd_HH:mm:ss")) - INFO - $($target) Failed" | Tee-Object -FilePath "$($log_file_name)" -Append
        Write-Output "$($(Get-Date).ToString("yyyy:MM:dd_HH:mm:ss")) - INFO - $($PSItem.ToString())" | Tee-Object -FilePath "$($log_file_name)" -Append
    }
}

# Iterate through secondary stored procedures
$secondary_stored_proc_names = @(
    "generateRows_RFS025",
    "generateRows_RFS041",
    "time_code_report_proc",
    "position_filledvacant_proc",
    "chart_account_proc",
    "organizational_unit_hierarchy_proc",
    "employee_agency_proc",
    "Trial_Balance_Proc"
)

foreach ($stored_proc_name in $secondary_stored_proc_names) {
    try {
        Write-Output "$($(Get-Date).ToString("yyyy:MM:dd_HH:mm:ss")) - INFO - Running $($stored_proc_name)..." | Tee-Object -FilePath "$($log_file_name)" -Append
        .\workflow.ps1 run_stored_proc $stored_proc_name | Tee-Object -FilePath "$($log_file_name)" -Append
    }
    catch {
        Write-Output "$($(Get-Date).ToString("yyyy:MM:dd_HH:mm:ss")) - INFO - $($stored_proc_name) Failed" | Tee-Object -FilePath "$($log_file_name)" -Append
        Write-Output "$($(Get-Date).ToString("yyyy:MM:dd_HH:mm:ss")) - INFO - $($PSItem.ToString())" | Tee-Object -FilePath "$($log_file_name)" -Append
    }
}

# Run a search for error messages and generate a summary file
.\workflow.ps1 summarize_daily_logs

# Update the job tracker table
.\workflow.ps1 update_job_tracker

Write-Output "$($(Get-Date).ToString("yyyy:MM:dd_HH:mm:ss")) - INFO - END" | Tee-Object -FilePath "$($log_file_name)" -Append

# Hop out of the working directory when finished
Pop-Location