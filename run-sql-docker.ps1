$ErrorActionPreference = 'Stop'
function Ensure-Docker { if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw "docker não encontrado" } }
function Compose-Up { & docker compose up -d }
function Wait-Pg {
  for ($i=0; $i -lt 40; $i++) {
    $r = & docker exec lcx-pg bash -lc "psql -U postgres -d postgres -c 'select 1'" 2>$null
    if ($LASTEXITCODE -eq 0) { return }
    Start-Sleep -Seconds 2
  }
  throw "pg indisponível"
}
function Wait-My {
  for ($i=0; $i -lt 40; $i++) {
    $r = & docker exec lcx-mysql bash -lc "mysql -uroot -pexample -e 'select 1'" 2>$null
    if ($LASTEXITCODE -eq 0) { return }
    Start-Sleep -Seconds 2
  }
  throw "mysql indisponível"
}
function Wait-Ms {
  for ($i=0; $i -lt 60; $i++) {
    $r = & docker exec lcx-mssql bash -lc "/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'Your_password123!' -Q 'select 1'" 2>$null
    if ($LASTEXITCODE -eq 0) { return }
    Start-Sleep -Seconds 3
  }
  throw "mssql indisponível"
}
function Run-Pg {
  & docker exec lcx-pg bash -lc "psql -U postgres -d postgres -v ON_ERROR_STOP=1 -f /sql/postgres/schema.sql"
  & docker exec lcx-pg bash -lc "psql -U postgres -d postgres -v ON_ERROR_STOP=1 -f /sql/postgres/analytics.sql"
  & docker exec lcx-pg bash -lc "psql -U postgres -d postgres -v ON_ERROR_STOP=1 -f /sql/postgres/rls.sql"
}
function Run-My {
  & docker exec lcx-mysql bash -lc "mysql -uroot -pexample -e \"source /sql/mysql/schema.sql\""
  & docker exec lcx-mysql bash -lc "mysql -uroot -pexample -e \"source /sql/mysql/analytics.sql\""
}
function Run-Ms {
  & docker exec lcx-mssql bash -lc "/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'Your_password123!' -b -i /sql/sqlserver/schema.sql"
  & docker exec lcx-mssql bash -lc "/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'Your_password123!' -b -i /sql/sqlserver/analytics.sql"
  & docker exec lcx-mssql bash -lc "/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'Your_password123!' -b -i /sql/sqlserver/rls.sql"
}
Ensure-Docker
Compose-Up
Wait-Pg
Wait-My
Wait-Ms
Run-Pg
Run-My
Run-Ms
Write-Host "docker runner concluído"
