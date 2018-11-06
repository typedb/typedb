#
# GRAKN.AI - THE KNOWLEDGE GRAPH
# Copyright (C) 2018 Grakn Labs Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

#!/usr/bin/env bash

set -ex # exit immediately when there's a failure. run the script in verbose mode (ie., print all the commands to stdout)

if [[ $# -ne 2 ]]; then
    echo "Error - needs two arguments: <github-username> <github-token>"
    exit 1
fi

if [[ "$(uname)" != "Darwin" ]] && [[ "$(uname)" != "Linux" ]]; then
    echo "Error - your platform ('$(uname)') isn't supported. Try Linux or OS X instead."
    exit 1
fi

# command-line arguments
github_user="$1"
github_token="$2"

# configurations
distribution_name="grakn-core-all"
distribution_zipfile_path="dist/grakn-core-all.zip"
distribution_version=`cat VERSION`
distribution_basedir_name="$distribution_name-$distribution_version"
distribution_zipfile_name="$distribution_basedir_name.zip"
github_repository="`grep github.repository deployment.properties | cut -d '=' -f 2`"
github_tag="v$distribution_version" # e.g, v1.5.0
ghr_osx_basedir_name="ghr_v0.10.2_darwin_386"
ghr_linux_basedir_name="ghr_v0.10.2_linux_386"
ghr_executable_file_name="ghr"
tmp_dir_name="tmp"

# assign variables which needs to be initialised according to the platform (ie., OS X or Linux)
if [ "$(uname)" == "Darwin" ]; then
    ghr_basedir_name="$ghr_osx_basedir_name"
    ghr_distribution_path="external/ghr_osx_zip/file/$ghr_basedir_name.zip"
    unpack_ghr_based_on_platform="unzip $ghr_distribution_path -d $tmp_dir_name/"
elif [ "$(uname)" == "Linux" ]; then
    ghr_basedir_name="$ghr_linux_basedir_name"
    ghr_distribution_path="external/ghr_linux_tar/file/$ghr_basedir_name.tar.gz"
    unpack_ghr_based_on_platform="tar -xf $ghr_distribution_path -C $tmp_dir_name/"
fi

# 1. initialise tmp directory
mkdir "$tmp_dir_name"
cp "$distribution_zipfile_path" "$tmp_dir_name/$distribution_zipfile_name"

# 2. create a draft release
$unpack_ghr_based_on_platform # unpack ghr - unzip if on Mac, or tar -xf if on Linux
"$tmp_dir_name/$ghr_basedir_name/$ghr_executable_file_name" -t "$github_token" -u "$github_user" -r "$github_repository" -delete -draft "$github_tag" "$tmp_dir_name/$distribution_zipfile_name"

# 3. cleanup tmp directory
rm -rf "$tmp_dir_name"