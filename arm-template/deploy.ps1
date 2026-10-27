<#
.SYNOPSIS
    Deploys Azure Data Factory and all resources using ARM template.

.DESCRIPTION
    Validates and deploys ARMTemplateForFactory.json to the target Azure resource group.
    Supports pre-flight validation mode and handles Managed Identity role assignments.

.EXAMPLE
    .\deploy.ps1 -ResourceGroupName "data_test" -FactoryName "demopiplineADF" -Environment "prod"
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $false)]
    [string]$ResourceGroupName = "data_test",

    [Parameter(Mandatory = $false)]
    [string]$FactoryName = "demopiplineADF",

    [Parameter(Mandatory = $false)]
    [ValidateSet("dev", "test", "prod")]
    [string]$Environment = "prod",

    [Parameter(Mandatory = $false)]
    [string]$Location = "eastus2",

    [Parameter(Mandatory = $false)]
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " Azure Data Factory ARM Deployment" -ForegroundColor Cyan
Write-Host " Factory Name:      $FactoryName" -ForegroundColor Cyan
Write-Host " Resource Group:    $ResourceGroupName" -ForegroundColor Cyan
Write-Host " Environment:       $Environment" -ForegroundColor Cyan
Write-Host " Location:          $Location" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$TemplateFile = Join-Path $ScriptDir "ARMTemplateForFactory.json"
$TemplateParametersFile = Join-Path $ScriptDir "ARMTemplateParametersForFactory.json"

if (-not (Test-Path $TemplateFile)) {
    throw "ARM Template not found at $TemplateFile"
}

Write-Host "`n[1/3] Validating ARM Template..." -ForegroundColor Yellow
$ValidationResult = az deployment group validate `
    --resource-group $ResourceGroupName `
    --template-file $TemplateFile `
    --parameters "@$TemplateParametersFile" `
    --parameters factoryName=$FactoryName environment=$Environment location=$Location `
    2>&1

if ($LASTEXITCODE -ne 0) {
    Write-Host "Validation warning or error:" -ForegroundColor Red
    Write-Host $ValidationResult
} else {
    Write-Host "Template syntax and parameter validation passed successfully." -ForegroundColor Green
}

if ($WhatIf) {
    Write-Host "`nRunning What-If analysis..." -ForegroundColor Yellow
    az deployment group what-if `
        --resource-group $ResourceGroupName `
        --template-file $TemplateFile `
        --parameters "@$TemplateParametersFile" `
        --parameters factoryName=$FactoryName environment=$Environment location=$Location
    exit 0
}

Write-Host "`n[2/3] Executing ARM Template deployment..." -ForegroundColor Yellow
$DeploymentOutput = az deployment group create `
    --resource-group $ResourceGroupName `
    --template-file $TemplateFile `
    --parameters "@$TemplateParametersFile" `
    --parameters factoryName=$FactoryName environment=$Environment location=$Location `
    --query "properties.outputs" `
    -o json

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n[3/3] Deployment succeeded!" -ForegroundColor Green
    Write-Host $DeploymentOutput
} else {
    Write-Host "`nDeployment failed with exit code $LASTEXITCODE." -ForegroundColor Red
    exit $LASTEXITCODE
}
