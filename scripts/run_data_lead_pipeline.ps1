param(
    [ValidateSet("1", "5", "10")] [string]$Scale = "5",
    [switch]$SkipDocker,
    [string]$DumpFile = ""
)

$ErrorActionPreference = "Stop"

function Invoke-CheckedCommand {
    param(
        [scriptblock]$Command,
        [string]$Description
    )

    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE"
    }
}

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Push-Location $projectRoot
try {

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env from .env.example"
}

$envFile = Get-Content ".env" | Where-Object { $_ -and -not $_.StartsWith("#") }
foreach ($line in $envFile) {
    $parts = $line -split "=", 2
    if ($parts.Count -eq 2) {
        [System.Environment]::SetEnvironmentVariable($parts[0], $parts[1])
        Set-Item -Path ("Env:" + $parts[0]) -Value $parts[1]
    }
}

$venvPython = Join-Path $projectRoot "venv\Scripts\python.exe"
$pythonExe = if (Test-Path $venvPython) { $venvPython } else { "python" }
Write-Host "Using Python interpreter: $pythonExe"

Write-Host "Installing Python dependencies..."
Invoke-CheckedCommand -Description "Dependency installation" -Command {
    & $pythonExe -m pip install -r requirements.txt
}

$hasDump = -not [string]::IsNullOrWhiteSpace($DumpFile)
if ($hasDump -and -not (Test-Path $DumpFile)) {
    throw "Dump file not found: $DumpFile"
}

if ($hasDump -and -not $SkipDocker) {
    Write-Host "Fast path: restoring Docker database from dump..."
    Invoke-CheckedCommand -Description "Dump restore" -Command {
        powershell -ExecutionPolicy Bypass -File scripts/restore_tpch_dump.ps1 -DumpFile $DumpFile
    }
    Write-Host "Pipeline complete via dump restore."
    exit 0
}

$rawDir = "data/raw/sf$Scale"
$cleanDir = "data/clean/sf$Scale"
$validationReport = "reports/validation_sf$Scale.json"

Write-Host "Generating TPC-H SF$Scale..."
Invoke-CheckedCommand -Description "TPC-H generation" -Command {
    & $pythonExe scripts/generate_tpch_duckdb.py --sf $Scale --out-dir $rawDir --force
}

Write-Host "Cleaning TPC-H SF$Scale..."
Invoke-CheckedCommand -Description "TPC-H cleaning" -Command {
    & $pythonExe scripts/clean_tpch.py --input-dir $rawDir --output-dir $cleanDir --force
}

Write-Host "Validating cleaned data SF$Scale..."
$metadataPath = Join-Path $rawDir "generation_metadata.json"
Invoke-CheckedCommand -Description "TPC-H validation" -Command {
    & $pythonExe scripts/validate_tpch.py --input-dir $cleanDir --report $validationReport --metadata $metadataPath
}

if (-not $SkipDocker) {
    Write-Host "Loading into Dockerized Postgres..."
    Invoke-CheckedCommand -Description "Postgres load" -Command {
        powershell -ExecutionPolicy Bypass -File scripts/load_to_postgres.ps1 -Scale $Scale
    }
}

Write-Host "Pipeline complete for SF$Scale"
}
finally {
    Pop-Location
}
