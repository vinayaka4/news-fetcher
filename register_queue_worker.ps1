$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$python = Join-Path $projectRoot ".venv\Scripts\python.exe"
$worker = Join-Path $projectRoot "queue_worker.py"
$taskName = "UPSC News Durable Queue Worker"

if (-not (Test-Path -LiteralPath $python)) {
    throw "Project Python environment not found: $python"
}

$action = New-ScheduledTaskAction -Execute $python `
    -Argument ('"' + $worker + '" --hydrate-missing-pib --drain') `
    -WorkingDirectory $projectRoot
$trigger = New-ScheduledTaskTrigger -Once -At ((Get-Date).AddMinutes(1)) `
    -RepetitionInterval (New-TimeSpan -Minutes 15) `
    -RepetitionDuration (New-TimeSpan -Days 3650)
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger `
    -Settings $settings -Description "Retries durable RSS, PIB, and Perplexity ingestion jobs" `
    -Force | Out-Null
Write-Output "Registered '$taskName' to drain durable jobs every 15 minutes."
