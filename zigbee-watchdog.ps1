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

# 'Continue' so a native exe (usbipd/wsl/docker) writing to stderr does NOT
# promote to a terminating error in PS 5.1. Cmdlets that must be caught use an
# explicit -ErrorAction Stop (Disable/Enable-PnpDevice, Get/Start-Service).
$ErrorActionPreference = 'Continue'

$HardwareId   = '10c4:ea60'                 # SONOFF Dongle Lite MG21 (CP210x)
$Container    = 'zigbee2mqtt'
$AutoAttachTask = 'ZigbeeDongleAutoAttach'
$FailThreshold = 2                           # consecutive failing checks before power-cycle
$PcBackoffSeconds = 1800                      # after a failed power-cycle, wait 30 min (avoid USB-chime spam)
$StateFile    = Join-Path $PSScriptRoot 'watchdog.state'
$PcBackoffFile = Join-Path $PSScriptRoot 'watchdog.pcbackoff'
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
    # Wrap everything: a failure in any step must not abort the whole watchdog
    # run (ErrorActionPreference is 'Stop' at script scope).
    try {
        Write-Log "WEDGE confirmed -> power-cycling dongle"

        # 1. Detach from WSL so Windows reclaims the device under its CP210x driver.
        #    (Do NOT touch usbipd.exe processes - that is the usbipd SERVICE now,
        #     and killing it aborts everything.)
        $line = usbipd list 2>$null | Select-String $HardwareId | Select-Object -First 1
        if ($line) {
            $busid = ($line.ToString().Trim() -split '\s+')[0]
            usbipd detach --busid $busid 2>$null
            Write-Log "detached busid $busid"
            Start-Sleep -Seconds 3
        }

        # 2. Power-cycle the chip via Windows PnP (disable then enable).
        $dev = Get-PnpDevice -ErrorAction SilentlyContinue |
               Where-Object { $_.InstanceId -like 'USB\VID_10C4&PID_EA60*' } | Select-Object -First 1
        if (-not $dev) { Write-Log "ERROR: dongle not found as PnP device, aborting"; return }

        Disable-PnpDevice -InstanceId $dev.InstanceId -Confirm:$false -ErrorAction Stop
        Write-Log "disabled $($dev.InstanceId)"
        Start-Sleep -Seconds 5
        Enable-PnpDevice  -InstanceId $dev.InstanceId -Confirm:$false -ErrorAction Stop
        Write-Log "enabled  $($dev.InstanceId)"
        Start-Sleep -Seconds 8

        # 3. Re-attach to WSL ourselves (don't depend on the auto-attach task,
        #    which may be disabled) and verify the serial node actually appears.
        usbipd attach --wsl --hardware-id $HardwareId 2>$null | Out-Null
        Start-Sleep -Seconds 4
        $node = (wsl -d docker-desktop -- ls /dev/ttyUSB0 2>$null)
        if ($node) {
            Write-Log "re-attached; /dev/ttyUSB0 present"
            docker restart $Container 2>$null | Out-Null
            Write-Log "restarted $Container"
            return $true
        }
        Write-Log "PnP power-cycle did NOT restore the serial node - chip needs a PHYSICAL replug"
        return $false
    } catch {
        Write-Log "power-cycle error: $($_.Exception.Message)"
        return $false
    }
}

# ------ Infrastructure check (runs elevated, so it can fix things autoattach can't) ------
# 1. The usbipd Windows service sometimes stops; while it's down EVERY attach
#    fails and nothing can recover. Restart it.
try {
    $svc = Get-Service usbipd -ErrorAction Stop
    if ($svc.Status -ne 'Running') {
        Start-Service usbipd -ErrorAction Stop
        Write-Log "usbipd service was $($svc.Status) -> started it"
        Start-Sleep -Seconds 3
    }
} catch {
    Write-Log "could not query/start usbipd service: $($_.Exception.Message)"
}

# 2. Ensure the dongle is attached to WSL (in case autoattach failed while the
#    service was down).
try {
    $line = usbipd list 2>$null | Select-String $HardwareId | Select-Object -First 1
    if ($line -and $line.ToString() -notmatch 'Attached') {
        usbipd attach --wsl --hardware-id $HardwareId 2>$null | Out-Null
        Write-Log "attached dongle (was not attached)"
        Start-Sleep -Seconds 3
        if ((docker inspect -f '{{.State.Status}}' $Container 2>$null) -match 'exited') {
            docker start $Container 2>$null | Out-Null
            Write-Log "started $Container after attach"
        }
    }
} catch {
    Write-Log "attach check failed: $($_.Exception.Message)"
}

# 3. Ensure the cp210x serial driver is loaded. The docker-desktop WSL kernel
#    ships the module but does NOT auto-load it after a restart - the actual root
#    cause of the recurring outages. modprobe is idempotent.
try {
    wsl -d docker-desktop -- modprobe cp210x 2>$null | Out-Null
    if ((wsl -d docker-desktop -- ls /dev/ttyUSB0 2>$null) -and
        ((docker inspect -f '{{.State.Status}}' $Container 2>$null) -match 'exited')) {
        docker start $Container 2>$null | Out-Null
        Write-Log "loaded cp210x + started $Container"
    }
} catch {}

# ------ Health check ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
    # Healthy --- reset the counter.
    if ((Get-FailCount) -ne 0) { Write-Log "healthy again; resetting counter" }
    Set-FailCount 0
    exit 0
}

if ($logWedge -or $state -in @('restarting','exited','dead')) {
    $n = (Get-FailCount) + 1
    Write-Log "dongle failure detected (state=$state, strike $n/$FailThreshold)"
    if ($n -lt $FailThreshold) { Set-FailCount $n; exit 0 }

    # Armed to power-cycle. But a PnP power-cycle has been proven NOT to fix a
    # genuine chip wedge (only a physical replug does), so after one fails we
    # back off $PcBackoffSeconds rather than chiming the USB every 2 minutes.
    $lastFail = 0
    if (Test-Path $PcBackoffFile) { try { $lastFail = [long](Get-Content $PcBackoffFile -Raw) } catch {} }
    $now = [long]([DateTimeOffset]::UtcNow.ToUnixTimeSeconds())
    if (($now - $lastFail) -lt $PcBackoffSeconds) {
        Write-Log "wedge persists but within power-cycle back-off; waiting for a PHYSICAL replug (quiet)"
        Set-FailCount 0
        exit 0
    }

    $restored = Invoke-PowerCycle
    if (-not $restored) { Set-Content -Path $PcBackoffFile -Value $now -Encoding ascii }
    else { Remove-Item $PcBackoffFile -ErrorAction SilentlyContinue }
    Set-FailCount 0
    exit 0
}

# Ambiguous (e.g. z2m mid-start, logs quiet, container running) --- hold steady.
exit 0
