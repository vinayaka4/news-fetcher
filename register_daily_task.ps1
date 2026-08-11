$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$runner = Join-Path $projectRoot "scheduled_run.py"
$python = Join-Path $projectRoot ".venv\Scripts\python.exe"
$taskName = "UPSC Current Affairs Daily Fetch"

$action = New-ScheduledTaskAction `
    -Execute $python `
    -Argument "`"$runner`""
# 04:00 IST is 18:30 Eastern during daylight time and 17:30 during standard
# time on the previous calendar day. Both triggers are safe: scheduled_run.py
# checks IST and only the correct one proceeds.
$triggers = @(
    (New-ScheduledTaskTrigger -Daily -At "5:30 PM"),
    (New-ScheduledTaskTrigger -Daily -At "6:30 PM")
)
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2)

Register-ScheduledTask `
    -TaskName $taskName `
    -Action $action `
    -Trigger $triggers `
    -Settings $settings `
    -Description "Run raw news ingestion daily at 04:00 IST (DST-safe)." `
    -Force | Out-Null

Write-Output "Registered '$taskName' for 04:00 IST every day (DST-safe)."
