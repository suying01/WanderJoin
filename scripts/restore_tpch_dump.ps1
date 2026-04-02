param(
    [Parameter(Mandatory=$true)] [string]$DumpFile,
    [string]$ComposeFile = "docker-compose.yml"
)

$ErrorActionPreference = "Stop"

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
docker compose -f $ComposeFile up -d postgres | Out-Null

Write-Host "Copying dump into container..."
docker compose -f $ComposeFile cp $DumpFile "postgres:/tmp/tpch_dump_input" | Out-Null

Write-Host "Restoring database from dump..."
if ($DumpFile.ToLower().EndsWith(".sql")) {
    docker compose -f $ComposeFile exec -T postgres psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB -f /tmp/tpch_dump_input
} else {
    docker compose -f $ComposeFile exec -T postgres pg_restore -U $env:POSTGRES_USER -d $env:POSTGRES_DB /tmp/tpch_dump_input
}

Write-Host "Running verification query..."
docker compose -f $ComposeFile exec -T postgres psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "SELECT 'region' AS table_name, COUNT(*) AS row_count FROM region UNION ALL SELECT 'nation', COUNT(*) FROM nation UNION ALL SELECT 'supplier', COUNT(*) FROM supplier UNION ALL SELECT 'customer', COUNT(*) FROM customer UNION ALL SELECT 'part', COUNT(*) FROM part UNION ALL SELECT 'partsupp', COUNT(*) FROM partsupp UNION ALL SELECT 'orders', COUNT(*) FROM orders UNION ALL SELECT 'lineitem', COUNT(*) FROM lineitem ORDER BY table_name;"

Write-Host "Restore complete."
