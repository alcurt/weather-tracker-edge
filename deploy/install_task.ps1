#Requires -RunAsAdministrator
<#
.SYNOPSIS
    将天气追踪边缘节点注册为开机自启的 Windows 计划任务。
.DESCRIPTION
    使用 pythonw.exe 无窗口启动 src/main.py，注册为 SYSTEM 任务，进程异常退出时
    每分钟重试，最多 3 次。重复执行会覆盖同名任务。
.EXAMPLE
    .\deploy\install_task.ps1
    .\deploy\install_task.ps1 -TaskName "WeatherTrackerEdgeNode" -ProjectRoot "C:\PythonProjects\GuangzhouWeatherTracker"
#>
[CmdletBinding()]
param(
    [string]$TaskName = "WeatherTrackerEdgeNode",
    [string]$ProjectRoot = ""
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($ProjectRoot)) {
    $ProjectRoot = Split-Path -Parent $PSScriptRoot
}

$root = (Resolve-Path -LiteralPath $ProjectRoot).Path
$pythonw = Join-Path $root ".venv\Scripts\pythonw.exe"
$entry = Join-Path $root "src\main.py"

if (-not (Test-Path -LiteralPath $pythonw)) {
    throw "找不到 pythonw.exe: $pythonw。请先在项目根目录执行 uv sync。"
}
if (-not (Test-Path -LiteralPath $entry)) {
    throw "找不到入口文件: $entry"
}

$action = New-ScheduledTaskAction `
    -Execute $pythonw `
    -Argument "`"$entry`"" `
    -WorkingDirectory $root

$trigger = New-ScheduledTaskTrigger -AtStartup

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -MultipleInstances IgnoreNew

if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -User "SYSTEM" `
    -RunLevel Highest `
    -Description "天气追踪边缘节点：开机无窗口启动，异常退出自动重启" | Out-Null

Write-Host "已注册计划任务: $TaskName"
Write-Host "  执行: $pythonw `"$entry`""
Write-Host "  工作目录: $root"
Write-Host "  健康检查: http://127.0.0.1:11451/api/health"
Write-Host "立即启动: Start-ScheduledTask -TaskName `"$TaskName`""
