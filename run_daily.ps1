$ErrorActionPreference = "Continue"
$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$python = Join-Path $projectRoot ".venv\Scripts\python.exe"
$logDirectory = Join-Path $projectRoot "logs"
$logFile = Join-Path $logDirectory ("daily-" + (Get-Date -Format "yyyy-MM-dd") + ".log")

New-Item -ItemType Directory -Force -Path $logDirectory | Out-Null
Set-Location $projectRoot

if (-not (Test-Path -LiteralPath $python)) {
    "$(Get-Date -Format o) ERROR: Project Python environment not found." | Add-Content -LiteralPath $logFile
    exit 1
}

$env:NEWS_ENABLED_SOURCES = "pib,times_of_india_top,times_of_india_india"

"$(Get-Date -Format o) Enqueuing daily RSS, PIB, and Perplexity jobs" | Add-Content -LiteralPath $logFile
& $python queue_worker.py --enqueue-daily --hydrate-missing-pib --drain *>> $logFile
$workerExitCode = $LASTEXITCODE

"$(Get-Date -Format o) Completed durable queue worker (exit=$workerExitCode)" | Add-Content -LiteralPath $logFile
if ($workerExitCode -ne 0) { exit 1 }
exit 0
