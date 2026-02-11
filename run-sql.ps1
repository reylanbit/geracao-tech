param(
  [ValidateSet('auto','postgres','mysql','sqlserver')]
  [string]$Engine = 'auto',
  [string]$ConnString = $env:NEON_URL
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Invoke-Postgres {
  if (Get-Command psql -ErrorAction SilentlyContinue) {
    Write-Host "==> Postgres encontrado. Executando scripts..."
    $common = @("-v","ON_ERROR_STOP=1")
    $dest = @()
    if ($ConnString) { $dest = @("-d", $ConnString) }
    & psql $common $dest -f "sql/postgres/schema.sql"
    & psql $common $dest -f "sql/postgres/analytics.sql"
    & psql $common $dest -f "sql/postgres/rls.sql"
    & psql $common $dest -f "sql/postgres/portfolio.sql"
    Write-Host "==> Postgres concluído."
  } else {
    Write-Warning "psql não encontrado. Instale PostgreSQL client ou defina PATH."
  }
}

function Invoke-MySQL {
  if (Get-Command mysql -ErrorAction SilentlyContinue) {
    Write-Host "==> MySQL encontrado. Executando scripts..."
    & mysql -e "source sql/mysql/schema.sql"
    & mysql -e "source sql/mysql/analytics.sql"
    Write-Host "==> MySQL concluído."
  } else {
    Write-Warning "mysql não encontrado. Instale MySQL client ou defina PATH."
  }
}

function Invoke-SqlServer {
  if (Get-Command sqlcmd -ErrorAction SilentlyContinue) {
    Write-Host "==> SQL Server encontrado. Executando scripts..."
    & sqlcmd -b -i "sql/sqlserver/schema.sql"
    & sqlcmd -b -i "sql/sqlserver/analytics.sql"
    & sqlcmd -b -i "sql/sqlserver/rls.sql"
    Write-Host "==> SQL Server concluído."
  } else {
    Write-Warning "sqlcmd não encontrado. Instale SQL Server Command Line Utilities."
  }
}

switch ($Engine) {
  'postgres' { Invoke-Postgres }
  'mysql'    { Invoke-MySQL }
  'sqlserver'{ Invoke-SqlServer }
  default {
    Invoke-Postgres
    Invoke-MySQL
    Invoke-SqlServer
  }
}

Write-Host "Runner finalizado."
