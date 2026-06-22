' run-hidden.vbs - launch a PowerShell script with no visible window.
' Usage:  wscript.exe run-hidden.vbs "C:\path\to\script.ps1"
' wscript itself is windowless; intWindowStyle 0 hides the spawned PowerShell.
Set sh = CreateObject("WScript.Shell")
cmd = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File """ & WScript.Arguments(0) & """"
sh.Run cmd, 0, False
