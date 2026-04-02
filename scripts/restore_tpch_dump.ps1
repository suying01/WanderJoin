param(
    [Parameter(Mandatory=$true)] [string]$DumpFile,
    [string]$ComposeFile = "docker-compose.yml"
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

if (-not (Test-Path $DumpFile)) {
    throw "Dump file not found: $DumpFile"
}

if (-not $env:POSTGRES_DB -or -not $env:POSTGRES_USER) {
    if (-not (Test-Path ".env")) {
        throw "Missing .env file and required POSTGRES_* environment variables."
    }

    $envFile = Get-Content ".env" | Where-Object { $_ -and -not $_.StartsWith("#") }
    foreach ($line in $envFile) {
        $parts = $line -split "=", 2
        if ($parts.Count -eq 2) {
            Set-Item -Path ("Env:" + $parts[0]) -Value $parts[1]
        }
    }
}

Write-Host "Starting docker services..."
Invoke-CheckedCommand -Description "Start Docker services" -Command {
    docker compose -f $ComposeFile up -d postgres | Out-Null
}

Write-Host "Copying dump into container..."
Invoke-CheckedCommand -Description "Copy dump into container" -Command {
    docker compose -f $ComposeFile cp $DumpFile "postgres:/tmp/tpch_dump_input" | Out-Null
}

Write-Host "Resetting public schema for clean restore..."
Invoke-CheckedCommand -Description "Reset schema" -Command {
    docker compose -f $ComposeFile exec -T postgres psql -v ON_ERROR_STOP=1 -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;"
}

Write-Host "Restoring database from dump..."
if ($DumpFile.ToLower().EndsWith(".sql")) {
    Invoke-CheckedCommand -Description "Restore SQL dump" -Command {
        docker compose -f $ComposeFile exec -T postgres psql -v ON_ERROR_STOP=1 -U $env:POSTGRES_USER -d $env:POSTGRES_DB -f /tmp/tpch_dump_input
    }
} else {
    Invoke-CheckedCommand -Description "Restore binary dump" -Command {
        docker compose -f $ComposeFile exec -T postgres pg_restore --clean --if-exists --no-owner --no-privileges -U $env:POSTGRES_USER -d $env:POSTGRES_DB /tmp/tpch_dump_input
    }
}

Write-Host "Running verification query..."
Invoke-CheckedCommand -Description "Verification query" -Command {
    docker compose -f $ComposeFile exec -T postgres psql -v ON_ERROR_STOP=1 -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "SELECT 'region' AS table_name, COUNT(*) AS row_count FROM region UNION ALL SELECT 'nation', COUNT(*) FROM nation UNION ALL SELECT 'supplier', COUNT(*) FROM supplier UNION ALL SELECT 'customer', COUNT(*) FROM customer UNION ALL SELECT 'part', COUNT(*) FROM part UNION ALL SELECT 'partsupp', COUNT(*) FROM partsupp UNION ALL SELECT 'orders', COUNT(*) FROM orders UNION ALL SELECT 'lineitem', COUNT(*) FROM lineitem ORDER BY table_name;"
}

Write-Host "Restore complete."
