<#
    zigbee-autoattach.ps1
    ---------------------
    Stateless, non-elevated keep-alive for the SONOFF Zigbee dongle.
    Run every minute by the "ZigbeeDongleAutoAttach" scheduled task.

    Each run:
      1. If the dongle is not currently Attached to WSL, attach it.
      2. If the serial node is up but the z2m container has exited, start it.

    Idempotent and short-lived — nothing stays running between invocations, so
    there is no daemon process to get wedged (the old --auto-attach approach did).
    Power-cycling a genuinely wedged chip is handled separately by
    zigbee-watchdog.ps1 (which needs elevation).
#>

$ErrorActionPreference = 'SilentlyContinue'

$HardwareId = '10c4:ea60'
$Container  = 'zigbee2mqtt'
$LogFile    = Join-Path $PSScriptRoot 'autoattach.log'

function Write-Log($msg) {
    Add-Content -Path $LogFile -Encoding utf8 `
        -Value ("{0}  {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $msg)
}

# 1. Ensure the dongle is attached to WSL.
$line = usbipd list 2>$null | Select-String $HardwareId | Select-Object -First 1
if (-not $line) {
    # Dongle not connected to the PC at all — nothing we can do in software.
    exit 0
}
if ($line.ToString() -notmatch 'Attached') {
    usbipd attach --wsl --hardware-id $HardwareId 2>$null | Out-Null
    Start-Sleep -Seconds 3
    Write-Log "attached dongle (was '$(($line.ToString().Trim() -split '\s+')[-1])')"
}

# 2. If the serial node is present but z2m exited, start it.
$node = (wsl -d docker-desktop -- ls /dev/ttyUSB0 2>$null)
if ($node) {
    $state = (docker inspect -f '{{.State.Status}}' $Container 2>$null)
    if ($state -and $state.Trim() -eq 'exited') {
        docker start $Container 2>$null | Out-Null
        Write-Log "started $Container (was exited, serial node present)"
    }
}

exit 0
