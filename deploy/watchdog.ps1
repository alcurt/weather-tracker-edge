#Requires -RunAsAdministrator
<#
.SYNOPSIS
    健康检查看门狗：探测 /api/health，失败则重启计划任务。
.DESCRIPTION
    可由用户自行创建每 5 分钟运行一次的计划任务，或手动执行。健康检查在启动
    宽限期内返回 200；超过宽限期仍有信源 10 分钟未完成 tick 时返回 503。
#>
[CmdletBinding()]
param(
    [string]$TaskName = "WeatherTrackerEdgeNode",
    [int]$Port = 11451,
    [string]$LogPath = (Join-Path (Split-Path -Parent $PSScriptRoot) "logs\watchdog.log")
)

$healthUrl = "http://127.0.0.1:$Port/api/health"
$healthy = $false
try {
    $response = Invoke-WebRequest -UseBasicParsing -Uri $healthUrl -TimeoutSec 15
    $healthy = ($response.StatusCode -eq 200)
} catch {
    $healthy = $false
}

if ($healthy) {
    exit 0
}

$logDir = Split-Path -Parent $LogPath
if (-not (Test-Path -LiteralPath $logDir)) {
    New-Item -ItemType Directory -Path $logDir -Force | Out-Null
}
$stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
Add-Content -LiteralPath $LogPath -Value "[$stamp] health check failed, restarting task $TaskName"

Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
Start-Sleep -Seconds 5
Start-ScheduledTask -TaskName $TaskName
