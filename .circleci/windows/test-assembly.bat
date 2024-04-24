@echo off

REM This Source Code Form is subject to the terms of the Mozilla Public
REM License, v. 2.0. If a copy of the MPL was not distributed with this
REM file, You can obtain one at https://mozilla.org/MPL/2.0/.


REM needs to be called such that software installed
REM by Chocolatey in prepare.bat is accessible
CALL refreshenv

REM build typedb-all-windows archive
bazel --output_user_root=C:/bzl test //test/assembly:assembly --test_output=streamed --enable_runfiles --test_env=PATH --java_language_version=11 --javacopt="--release 11"

:error
IF %errorlevel% NEQ 0 EXIT /b %errorlevel%
