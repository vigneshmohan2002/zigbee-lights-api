<#
    zigbee-watchdog.ps1
    -------------------
    Single-shot health check for the SONOFF Zigbee dongle, run every minute by
    the scheduled task "ZigbeeDongleWatchdog" (registered by install-watchdog.ps1).

    It distinguishes the two failure modes:
      * USB link dropped  -> the ZigbeeDongleAutoAttach task already handles this.
      * Chip wedged       -> z2m logs show "Failure to connect" / sequence:-1 with
                             no successful start. Only a power-cycle fixes this, so
                             this script disables+enables the device via Windows PnP
                             (requires the task to run elevated).

    A power-cycle only fires after the wedge persists across $FailThreshold
    consecutive checks, so transient blips don't trigger it.
#>

$ErrorActionPreference = 'Stop'

$HardwareId   = '10c4:ea60'                 # SONOFF Dongle Lite MG21 (CP210x)
$Container    = 'zigbee2mqtt'
$AutoAttachTask = 'ZigbeeDongleAutoAttach'
$FailThreshold = 2                           # consecutive failing checks before power-cycle
$StateFile    = Join-Path $PSScriptRoot 'watchdog.state'
$LogFile      = Join-Path $PSScriptRoot 'watchdog.log'

function Write-Log($msg) {
    $line = "{0}  {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $msg
    Add-Content -Path $LogFile -Value $line -Encoding utf8
}

function Get-FailCount {
    if (Test-Path $StateFile) { try { return [int](Get-Content $StateFile -Raw) } catch { return 0 } }
    return 0
}
function Set-FailCount($n) { Set-Content -Path $StateFile -Value $n -Encoding ascii }

function Invoke-PowerCycle {
    Write-Log "WEDGE confirmed -> power-cycling dongle"

    # 1. Pause the auto-attach watcher so it doesn't grab the device mid-cycle.
    try { Stop-ScheduledTask -TaskName $AutoAttachTask -ErrorAction SilentlyContinue } catch {}
    Get-Process usbipd -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2

    # 2. Return the device to the Windows host, then power-cycle it via PnP.
    $line  = usbipd list 2>$null | Select-String $HardwareId | Select-Object -First 1
    if ($line) {
        $busid = ($line.ToString().Trim() -split '\s+')[0]
        usbipd detach --busid $busid 2>$null
        Write-Log "detached busid $busid"
        Start-Sleep -Seconds 3
    }

    $dev = Get-PnpDevice -PresentOnly -ErrorAction SilentlyContinue |
           Where-Object { $_.InstanceId -like 'USB\VID_10C4&PID_EA60*' } | Select-Object -First 1
    if (-not $dev) { Write-Log "ERROR: dongle not found as PnP device, aborting power-cycle"; return }

    Disable-PnpDevice -InstanceId $dev.InstanceId -Confirm:$false
    Write-Log "disabled $($dev.InstanceId)"
    Start-Sleep -Seconds 4
    Enable-PnpDevice  -InstanceId $dev.InstanceId -Confirm:$false
    Write-Log "enabled  $($dev.InstanceId)"
    Start-Sleep -Seconds 6

    # 3. Resume the auto-attach watcher; it re-attaches the now-healthy chip to WSL.
    try { Start-ScheduledTask -TaskName $AutoAttachTask -ErrorAction SilentlyContinue } catch {}
    Start-Sleep -Seconds 5

    # 4. Kick z2m so it reconnects immediately instead of on its next restart loop.
    docker restart $Container 2>$null | Out-Null
    Write-Log "restarted $Container"
}

# ── Health check ────────────────────────────────────────────────────────────
# Read a wide window so we still see errors when the container is in Docker's
# restart-backoff (during which it produces no new log lines).
try {
    $logs = (docker logs $Container --since 180s 2>&1 | Out-String)
} catch {
    Write-Log "could not read docker logs ($($_.Exception.Message)); skipping"
    exit 0
}

# Container run-state is a second signal: a 'restarting'/'exited' container is
# failing even if its log window happens to be quiet.
try { $state = (docker inspect -f '{{.State.Status}}' $Container 2>$null).Trim() } catch { $state = 'unknown' }

$failPatterns = @(
    'Failure to connect',
    '"sequence":-1',
    'Failed to start zigbee-herdsman',
    'No such device or address',
    'cannot open /dev/ttyUSB'
)
$logWedge = $false
foreach ($p in $failPatterns) { if ($logs -match $p) { $logWedge = $true; break } }

if ($state -eq 'running' -and ($logs -match 'Zigbee2MQTT started') -and -not $logWedge) {
    # Healthy — reset the counter.
    if ((Get-FailCount) -ne 0) { Write-Log "healthy again; resetting counter" }
    Set-FailCount 0
    exit 0
}

if ($logWedge -or $state -in @('restarting','exited','dead')) {
    $n = (Get-FailCount) + 1
    Write-Log "dongle failure detected (state=$state, strike $n/$FailThreshold)"
    if ($n -ge $FailThreshold) {
        Invoke-PowerCycle
        Set-FailCount 0
    } else {
        Set-FailCount $n
    }
    exit 0
}

# Ambiguous (e.g. z2m mid-start, logs quiet, container running) — hold steady.
exit 0
