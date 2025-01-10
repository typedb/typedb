@echo off
REM This Source Code Form is subject to the terms of the Mozilla Public
REM License, v. 2.0. If a copy of the MPL was not distributed with this
REM file, You can obtain one at https://mozilla.org/MPL/2.0/.

REM install dependencies needed for build
git apply .circleci\windows\git.patch
choco install .circleci\windows\dependencies.config  --limit-output --yes --no-progress

REM permanently set variables for Bazel build
SETX BAZEL_SH "C:\Program Files\Git\usr\bin\bash.exe"

