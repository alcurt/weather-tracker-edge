#Requires -RunAsAdministrator
<#
.SYNOPSIS
    卸载天气追踪边缘节点的计划任务。
#>
[CmdletBinding()]
param(
    [string]$TaskName = "WeatherTrackerEdgeNode"
)

$ErrorActionPreference = "Stop"
if (-not (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue)) {
    Write-Host "计划任务不存在: $TaskName"
    return
}

Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
Write-Host "已卸载计划任务: $TaskName"
