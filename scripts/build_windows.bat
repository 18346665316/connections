@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

echo ===================================================
echo   正在构建「邻传 (Connections)」Windows 生产版本...
echo ===================================================

call flutter pub get
if %ERRORLEVEL% neq 0 (
    echo [错误] flutter pub get 失败
    exit /b %ERRORLEVEL%
)

call flutter build windows --release
if %ERRORLEVEL% neq 0 (
    echo [错误] Windows Release 构建失败，请确认已安装 Visual Studio C++ 桌面开发组件
    exit /b %ERRORLEVEL%
)

if not exist "dist" mkdir "dist"
powershell -Command "Compress-Archive -Path 'build\windows\x64\runner\Release\*' -DestinationPath 'dist\邻传-Windows-x64-v1.0.0.zip' -Force"

echo.
echo [完成] Windows 生产版本已生成：
echo   - 可执行目录: build\windows\x64\runner\Release\connections.exe
echo   - 发行压缩包: dist\邻传-Windows-x64-v1.0.0.zip
pause
