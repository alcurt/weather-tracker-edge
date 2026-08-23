@echo off
:: 强制启用 UTF-8 编码，防止老旧 Windows 终端打印中文乱码
chcp 65001 >nul
setlocal enabledelayedexpansion

echo =====================================================
echo [运维] 正在检索局域网 11451 端口占用情况...
echo =====================================================

:: 1. 精准捕获占用 11451 端口且处于监听状态的 PID
set "TARGET_PID="
for /f "tokens=5" %%a in ('netstat -aon ^| findstr :11451 ^| findstr LISTENING') do (
    set "TARGET_PID=%%a"
)

if "%TARGET_PID%"==="" (
    echo [提示] 局域网端口 11451 未被占用，边缘节点未在运行。
    goto end
)

echo [锁定] 发现目标节点进程，PID 为: !TARGET_PID!

:: 2. 第一阶段：尝试发送温和的关闭信号 (WM_CLOSE)，允许 Python 触发异常捕获并安全释放管道
echo [动作] 尝试向进程树下发温和终止指令...
taskkill /PID !TARGET_PID! /T >nul 2>&1

:: 给予 5 秒钟的物理缓冲时间，让当前正在压制的 FFmpeg 尾帧或原子游标完成落盘
timeout /t 5 /nobreak >nul

:: 再查一次端口，确认温和终止是否生效
set "CHECK_PID="
for /f "tokens=5" %%a in ('netstat -aon ^| findstr :11451 ^| findstr LISTENING') do (
    set "CHECK_PID=%%a"
)

:: 3. 第二阶段：如果 5 秒后进程依然顽固占线，说明可能陷入死循环，立刻启动内核级硬猎杀
if not "%CHECK_PID%"=="" (
    echo [警告] 目标进程未能在时限内释放资源，启动强力强杀防线...
    taskkill /F /PID !TARGET_PID! /T >nul 2>&1
    echo [成功] 已通过内核防御强行收割进程树。
) else (
    echo [成功] 边缘节点所有后台资源已优雅释放。
)

:end
echo =====================================================
pause