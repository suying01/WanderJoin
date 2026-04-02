param(
    [Parameter(Mandatory=$true)] [string]$Scale,
    [string]$ComposeFile = "docker-compose.yml"
)

$ErrorActionPreference = "Stop"

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

$cleanDir = Join-Path "data/clean" "sf$Scale"
if (-not (Test-Path $cleanDir)) {
    throw "Clean directory not found: $cleanDir"
}

Write-Host "Starting docker services..."
docker compose -f $ComposeFile up -d postgres | Out-Null

Write-Host "Copying SQL files into container..."
docker compose -f $ComposeFile cp "sql/schema_tpch.sql" "postgres:/tmp/schema_tpch.sql" | Out-Null
docker compose -f $ComposeFile cp "sql/verification_query.sql" "postgres:/tmp/verification_query.sql" | Out-Null

Write-Host "Applying schema..."
docker compose -f $ComposeFile exec -T postgres psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB -f /tmp/schema_tpch.sql

Write-Host "Copying cleaned data into container..."
docker compose -f $ComposeFile exec -T postgres mkdir -p /tmp/tpch/sf$Scale
docker compose -f $ComposeFile cp "$cleanDir/." "postgres:/tmp/tpch/sf$Scale/" | Out-Null

Write-Host "Loading cleaned .tbl files..."
$copySql = @"
TRUNCATE TABLE lineitem, orders, partsupp, customer, supplier, part, nation, region;
\copy region FROM '/tmp/tpch/sf$Scale/region.tbl' DELIMITER '|' CSV;
\copy nation FROM '/tmp/tpch/sf$Scale/nation.tbl' DELIMITER '|' CSV;
\copy supplier FROM '/tmp/tpch/sf$Scale/supplier.tbl' DELIMITER '|' CSV;
\copy customer FROM '/tmp/tpch/sf$Scale/customer.tbl' DELIMITER '|' CSV;
\copy part FROM '/tmp/tpch/sf$Scale/part.tbl' DELIMITER '|' CSV;
\copy partsupp FROM '/tmp/tpch/sf$Scale/partsupp.tbl' DELIMITER '|' CSV;
\copy orders FROM '/tmp/tpch/sf$Scale/orders.tbl' DELIMITER '|' CSV;
\copy lineitem FROM '/tmp/tpch/sf$Scale/lineitem.tbl' DELIMITER '|' CSV;
"@

$copySqlPath = Join-Path $PSScriptRoot "_copy_tpch.sql"
$copySql | Out-File -FilePath $copySqlPath -Encoding utf8

docker compose -f $ComposeFile cp $copySqlPath "postgres:/tmp/copy_tpch.sql" | Out-Null

docker compose -f $ComposeFile exec -T postgres psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB -f /tmp/copy_tpch.sql

Remove-Item $copySqlPath -Force

Write-Host "Running verification query..."
docker compose -f $ComposeFile exec -T postgres psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB -f /tmp/verification_query.sql

Write-Host "Load complete for SF$Scale."
