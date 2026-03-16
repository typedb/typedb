@echo off
REM This Source Code Form is subject to the terms of the Mozilla Public
REM License, v. 2.0. If a copy of the MPL was not distributed with this
REM file, You can obtain one at https://mozilla.org/MPL/2.0/.

REM Clone dependency repos alongside typedb and configure local_path_override
REM so that Bazel uses these local checkouts instead of fetching from GitHub.
REM
REM Requires: REPO_GITHUB_TOKEN environment variable (already configured in CircleCI)
REM
REM Expected layout after this script:
REM   ~/typedb/          (this repo, already checked out by CircleCI)
REM   ~/dependencies/
REM   ~/bazel-distribution/
REM   ~/typedb-protocol/
REM   ~/typeql/
REM   ~/typedb-behaviour/

set BRANCH=bazel-8-upgrade

REM Clone each dependency repo on the bazel-8-upgrade branch
call :clone_repo dependencies        typedb/dependencies         || exit /b 1
call :clone_repo bazel-distribution  typedb/bazel-distribution   || exit /b 1
call :clone_repo typedb-protocol     typedb/typedb-protocol      || exit /b 1
call :clone_repo typeql              typedb/typeql                || exit /b 1
call :clone_repo typedb-behaviour    typedb/typedb-behaviour      || exit /b 1

REM Append local_path_override directives to MODULE.bazel
echo. >> MODULE.bazel
echo # Local dependency overrides for CI debugging >> MODULE.bazel
echo local_path_override( >> MODULE.bazel
echo     module_name = "typedb_dependencies", >> MODULE.bazel
echo     path = "../dependencies", >> MODULE.bazel
echo ) >> MODULE.bazel
echo local_path_override( >> MODULE.bazel
echo     module_name = "typedb_bazel_distribution", >> MODULE.bazel
echo     path = "../bazel-distribution", >> MODULE.bazel
echo ) >> MODULE.bazel
echo local_path_override( >> MODULE.bazel
echo     module_name = "typedb_protocol", >> MODULE.bazel
echo     path = "../typedb-protocol", >> MODULE.bazel
echo ) >> MODULE.bazel
echo local_path_override( >> MODULE.bazel
echo     module_name = "typeql", >> MODULE.bazel
echo     path = "../typeql", >> MODULE.bazel
echo ) >> MODULE.bazel
echo local_path_override( >> MODULE.bazel
echo     module_name = "typedb_behaviour", >> MODULE.bazel
echo     path = "../typedb-behaviour", >> MODULE.bazel
echo ) >> MODULE.bazel

echo.
echo Local dependency overrides configured in MODULE.bazel
exit /b 0

:clone_repo
REM %1 = repo directory name, %2 = GitHub org/repo
echo.
echo Cloning %1 (branch: %BRANCH%)...
git clone --branch %BRANCH% --single-branch https://%REPO_GITHUB_TOKEN%@github.com/%2.git ..\%1
if errorlevel 1 (
    echo ERROR: Failed to clone %1 on branch %BRANCH%
    exit /b 1
)
exit /b 0
