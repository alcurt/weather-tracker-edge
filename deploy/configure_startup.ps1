#Requires -Version 5.1
<#
.SYNOPSIS
    选择天气追踪边缘节点的启动方式：手动启动（默认）或开机自启。
.DESCRIPTION
    不带参数运行时进入交互选择，默认是手动启动；只有选择 2 才会注册计划任务。
    开机自启需要管理员权限。
.EXAMPLE
    .\deploy\configure_startup.ps1
    .\deploy\configure_startup.ps1 -Mode manual
    .\deploy\configure_startup.ps1 -Mode auto
    .\deploy\configure_startup.ps1 -Mode status
#>
[CmdletBinding()]
param(
    [string]$Mode = "",
    [string]$TaskName = "WeatherTrackerEdgeNode",
    [string]$ProjectRoot = ""
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($ProjectRoot)) {
    $ProjectRoot = Split-Path -Parent $PSScriptRoot
}

function Test-IsAdmin {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Get-NodeTask {
    return Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
}

function Show-StartupMode {
    $task = Get-NodeTask
    if ($null -eq $task) {
        Write-Host "当前启动方式: 手动启动（默认，无计划任务）"
        Write-Host "  手动启动: 双击 start_node.vbs"
    } else {
        Write-Host "当前启动方式: 开机自启（计划任务 $TaskName，状态 $($task.State)）"
    }
}

if ([string]::IsNullOrWhiteSpace($Mode)) {
    Show-StartupMode
    Write-Host ""
    Write-Host "1) 手动启动（默认，不注册计划任务）"
    Write-Host "2) 开机自启（注册 SYSTEM 计划任务）"
    $choice = Read-Host "请选择 [1]"
    if ([string]::IsNullOrWhiteSpace($choice) -or $choice -eq "1") {
        $Mode = "manual"
    } elseif ($choice -eq "2") {
        $Mode = "auto"
    } else {
        throw "无效选择: $choice"
    }
}

switch ($Mode.ToLowerInvariant()) {
    "status" {
        Show-StartupMode
    }
    "manual" {
        $task = Get-NodeTask
        if ($null -ne $task) {
            if (-not (Test-IsAdmin)) {
                throw "计划任务存在，切换为手动启动需要管理员权限。请用管理员 PowerShell 执行: .\deploy\configure_startup.ps1 -Mode manual"
            }
            Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
            Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
            Write-Host "已移除计划任务，切换为手动启动。"
        } else {
            Write-Host "已是手动启动（默认），未注册计划任务。"
        }
        Write-Host "启动方式: 双击 start_node.vbs"
    }
    "auto" {
        if (-not (Test-IsAdmin)) {
            throw "注册开机自启需要管理员权限。请用管理员 PowerShell 重新执行。"
        }
        & (Join-Path $PSScriptRoot "install_task.ps1") -TaskName $TaskName -ProjectRoot $ProjectRoot
    }
    default {
        throw "未知模式: $Mode。可选值: manual / auto / status"
    }
}
