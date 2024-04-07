param (
    [String] $ScriptRoot=$PSScriptRoot
)

# Declare globals
$LOG_DIR = "$(Get-Location)/logs"

# Hop into the proper directory
Push-Location $ScriptRoot

# Iterate through subject areas
$targets = @(
    # "account",
    # "account_analysis_settings",
    # "accounting_entity",
    # "billing_invoice",
    # "budget_change_order",
    # "budget_change_order_line",
    # "company_customer",
    # "contract",
    # "customer",
    # "customer_bill_to",
    # "finance_dimension_01",
    # "finance_dimension_02",
    # "finance_dimension_03",
    # "finance_dimension_04",
    # "finance_dimension_05",
    # "finance_dimension_06",
    # "finance_dimension_07",
    # "finance_dimension_10",
    # "general_ledger_chart_account",
    # "general_ledger_journal_control"
    "employee_deduction",
    "employee_payment"
)

foreach ($target in $targets) {
    try {
        .\workflow.ps1 load_pipeline $target 
    }
    catch {
        Write-Error "$($target) Failed"
        Write-Error "$($PSItem.ToString())"
    }
}

# Hop out of the working directory when finished
Pop-Location