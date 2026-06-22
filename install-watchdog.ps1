<#
    install-watchdog.ps1
    --------------------
    Registers the "ZigbeeDongleWatchdog" scheduled task that runs
    zigbee-watchdog.ps1 every minute, elevated, to auto-recover a wedged dongle.

    RUN THIS ONCE IN AN ADMINISTRATOR POWERSHELL:
        powershell -ExecutionPolicy Bypass -File install-watchdog.ps1
#>

$ErrorActionPreference = 'Stop'

# Must be elevated — PnP disable/enable and a Highest-privilege task both need admin.
$id = [Security.Principal.WindowsIdentity]::GetCurrent()
$pr = New-Object Security.Principal.WindowsPrincipal($id)
if (-not $pr.IsInRole([Security.Principal.WindowsBuiltinRole]::Administrator)) {
    Write-Host "ERROR: run this in an elevated (Administrator) PowerShell." -ForegroundColor Red
    exit 1
}

$TaskName = 'ZigbeeDongleWatchdog'
$Script   = Join-Path $PSScriptRoot 'zigbee-watchdog.ps1'
$Vbs      = Join-Path $PSScriptRoot 'run-hidden.vbs'
if (-not (Test-Path $Script)) { Write-Host "ERROR: $Script not found." -ForegroundColor Red; exit 1 }
if (-not (Test-Path $Vbs))    { Write-Host "ERROR: $Vbs not found."    -ForegroundColor Red; exit 1 }

# Launch via wscript + run-hidden.vbs so no console window ever appears.
$action = New-ScheduledTaskAction -Execute 'wscript.exe' -Argument "`"$Vbs`" `"$Script`""

# Start shortly after logon, then repeat every minute forever.
$trigger = New-ScheduledTaskTrigger -Once -At ((Get-Date).AddMinutes(1)) `
    -RepetitionInterval (New-TimeSpan -Minutes 1)
$logon   = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME

$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -StartWhenAvailable -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 10)

# Interactive + Highest => runs elevated, with access to the user's Docker pipe,
# but only while the user is logged on (Docker Desktop / WSL need the session too).
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Highest

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger @($trigger, $logon) `
    -Settings $settings -Principal $principal `
    -Description 'Detects a wedged SONOFF Zigbee dongle in z2m logs and power-cycles it via Windows PnP.' `
    -Force | Out-Null

Write-Host "Registered scheduled task '$TaskName'." -ForegroundColor Green
Get-ScheduledTask -TaskName $TaskName | Select-Object TaskName, State
Write-Host "Log file: $(Join-Path $PSScriptRoot 'watchdog.log')"
