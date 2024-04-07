param (
    [String] $ScriptRoot=$PSScriptRoot
)

# Hop into the proper directory
Push-Location $ScriptRoot

# Iterate through subject areas
$targets = @(
    "time_report_daily"
)

foreach ($target in $targets) {
    try {
        .\workflow.ps1 load_pipeline_subject_area $target
    }
    catch {
        Write-Output "$($target) Failed"
        Write-Output "$($PSItem.ToString())"
    }
}

# Iterate through secondary stored procedures
$secondary_stored_proc_names = @(
    "time_code_report_proc"
)

foreach ($stored_proc_name in $secondary_stored_proc_names) {
    try {
        .\workflow.ps1 run_stored_proc $stored_proc_name
    }
    catch {
        Write-Output "$($stored_proc_name) Failed"
        Write-Output "$($PSItem.ToString())"
    }
}

# Hop out of the working directory when finished
Pop-Location