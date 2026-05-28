@echo off
REM This Source Code Form is subject to the terms of the Mozilla Public
REM License, v. 2.0. If a copy of the MPL was not distributed with this
REM file, You can obtain one at https://mozilla.org/MPL/2.0/.

REM needs to be called such that software installed
REM by Chocolatey in prepare.bat is accessible
CALL refreshenv

bazel --windows_enable_symlinks test --config=ci --test_output=errors ^
    //admin/client:test_crate_client ^
    //server:test_admin_service
if %errorlevel% neq 0 exit /b %errorlevel%
