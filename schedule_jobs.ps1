# # Nightly ETL
# ### Fetch the path of the target script
# $run_script_path = Join-Path -Path $PSScriptRoot -ChildPath "\run.ps1"

# ### Define the timing of the trigger (accomodate server timezone six hour difference)
# $T = New-JobTrigger -Daily -At "08:00 AM" 

# ### Register the job
# Register-ScheduledJob -Name "Nightly_ETL" -FilePath $run_script_path -Trigger $T -ArgumentList $PSScriptRoot

################################################################################

# Hourly WFM ETL
### Fetch the path of the target script
$run_script_path = Join-Path -Path $PSScriptRoot -ChildPath "\wfm_hourly.ps1"
$T = New-JobTrigger -Daily -At "02:15 PM" 
Register-ScheduledJob -Name "Hourly_WFM_08_15" -FilePath $run_script_path -Trigger $T -ArgumentList $PSScriptRoot

$T = New-JobTrigger -Daily -At "04:15 PM" 
Register-ScheduledJob -Name "Hourly_WFM_10_15" -FilePath $run_script_path -Trigger $T -ArgumentList $PSScriptRoot

$T = New-JobTrigger -Daily -At "06:15 PM" 
Register-ScheduledJob -Name "Hourly_WFM_12_00" -FilePath $run_script_path -Trigger $T -ArgumentList $PSScriptRoot

$T = New-JobTrigger -Daily -At "08:15 PM" 
Register-ScheduledJob -Name "Hourly_WFM_02_15" -FilePath $run_script_path -Trigger $T -ArgumentList $PSScriptRoot

$T = New-JobTrigger -Daily -At "10:15 PM" 
Register-ScheduledJob -Name "Hourly_WFM_04_15" -FilePath $run_script_path -Trigger $T -ArgumentList $PSScriptRoot

$T = New-JobTrigger -Daily -At "12:15 AM" 
Register-ScheduledJob -Name "Hourly_WFM_06_15" -FilePath $run_script_path -Trigger $T -ArgumentList $PSScriptRoot