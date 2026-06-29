<#
    zigbee-autoattach.ps1
    ---------------------
    Stateless, non-elevated keep-alive for the SONOFF Zigbee dongle.
    Run every minute by the "ZigbeeDongleAutoAttach" scheduled task.

    Each run:
      1. If the dongle is not Attached to WSL, attach it --- BUT verify the serial
         node actually appears. If it doesn't (wedged chip: attach won't stick),
         back off so we don't re-attach every minute and chime the USB sound
         endlessly. Only a power-cycle fixes a wedge (physical replug, or the
         elevated zigbee-watchdog.ps1).
      2. If the serial node is up but the z2m container has exited, start it.

    Idempotent and short-lived. A small state file remembers consecutive failed
    attaches and the time of the last attempt so we can back off.
#>

$ErrorActionPreference = 'SilentlyContinue'

$HardwareId   = '10c4:ea60'
$Container    = 'zigbee2mqtt'
$LogFile      = Join-Path $PSScriptRoot 'autoattach.log'
$StateFile    = Join-Path $PSScriptRoot 'autoattach.state'
$FailsBeforeBackoff = 2          # noisy attempts allowed before backing off
$BackoffSeconds     = 900        # 15 min of silence once a wedge is detected

function Write-Log($msg) {
    Add-Content -Path $LogFile -Encoding utf8 `
        -Value ("{0}  {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $msg)
}

# state = "<fails>|<lastAttemptUnixSeconds>"
function Get-State {
    if (Test-Path $StateFile) {
        $parts = (Get-Content $StateFile -Raw).Trim() -split '\|'
        if ($parts.Count -eq 2) { return [pscustomobject]@{ Fails=[int]$parts[0]; Last=[long]$parts[1] } }
    }
    return [pscustomobject]@{ Fails=0; Last=0 }
}
function Set-State($fails, $last) { Set-Content -Path $StateFile -Value "$fails|$last" -Encoding ascii }
function Now-Unix { [long]([DateTimeOffset]::UtcNow.ToUnixTimeSeconds()) }

function Test-Node {
    # True if /dev/ttyUSB0 exists in the docker-desktop WSL distro.
    $r = (wsl -d docker-desktop -- ls /dev/ttyUSB0 2>$null)
    return [bool]$r
}

function Start-Z2mIfExited {
    if ((docker inspect -f '{{.State.Status}}' $Container 2>$null) -match 'exited') {
        docker start $Container 2>$null | Out-Null
        Write-Log "started $Container (serial node present)"
    }
}

function Ensure-Cp210x {
    # THE recurring fix: the docker-desktop WSL kernel ships the cp210x module
    # (.../usb/serial/cp210x.ko) but does NOT auto-load it after a WSL restart,
    # so the dongle enumerates with no /dev/ttyUSB0. modprobe is idempotent.
    wsl -d docker-desktop -- modprobe cp210x 2>$null | Out-Null
}

# ------ Is the dongle even plugged in? ------
$line = usbipd list 2>$null | Select-String $HardwareId | Select-Object -First 1
if (-not $line) { exit 0 }   # not connected to the PC - nothing to do

# ------ Attach to WSL if needed (back off on genuine repeated failures) ------
if ($line.ToString() -notmatch 'Attached') {
    $state = Get-State
    $now   = Now-Unix
    if ($state.Fails -ge $FailsBeforeBackoff -and ($now - $state.Last) -lt $BackoffSeconds) {
        exit 0   # within the quiet window - no attach, no sound
    }
    usbipd attach --wsl --hardware-id $HardwareId 2>$null | Out-Null
    Start-Sleep -Seconds 3
}

# ------ Ensure the serial driver is loaded, then check the node ------
Ensure-Cp210x
Start-Sleep -Seconds 1

if (Test-Node) {
    if ((Get-State).Fails -ne 0) { Write-Log "node present after recovery" }
    Set-State 0 0                # clear any back-off
    Start-Z2mIfExited
    exit 0
}

# Node still missing even after attach + modprobe: a genuine fault (e.g. dongle
# unresponsive). Back off so we don't churn/chime every minute.
$state = Get-State
$fails = $state.Fails + 1
Set-State $fails (Now-Unix)
if ($fails -ge $FailsBeforeBackoff) {
    Write-Log "no /dev/ttyUSB0 after attach+modprobe; backing off ${BackoffSeconds}s after $fails tries - may need a physical replug"
} else {
    Write-Log "no /dev/ttyUSB0 after attach+modprobe (try $fails/$FailsBeforeBackoff)"
}
exit 0
