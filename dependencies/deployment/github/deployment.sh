#!/usr/bin/env bash

if [ $# -ne 2 ]; then
    echo "Usage: bazel run //:deploy-github -- <github-username> <github-token>"
    exit 1
fi

set -ex # exit immediately when there's a failure. run the script in verbose mode (ie., print all the commands to stdout)

# command-line arguments
github_user="$1"
github_token="$2"

# configurations
github_repository="cat deployment.properties | grep github.repository | cut -d '=' -f 2"
distribution_name="grakn-core-all"
distribution_zipfile_path="dist/grakn-core-all.zip"
distribution_version=`cat VERSION`
distribution_basedir_name="$distribution_name-$distribution_version"
distribution_zipfile_name="$distribution_basedir_name.zip"
github_tag="v$distribution_version" # e.g, v1.5.0
ghr_executable_file_name="ghr"
ghr_basedir_name="ghr_v0.10.2_darwin_386"
ghr_zipfile_path="external/ghr/file/$ghr_basedir_name.zip"
tmp_dir_name="tmp"

# 1. initialise tmp directory
mkdir "$tmp_dir_name"
cp "$distribution_zipfile_path" "$tmp_dir_name/$distribution_zipfile_name"

# 2. create a draft release
unzip "$ghr_zipfile_path" -d "$tmp_dir_name/"
"$tmp_dir_name/$ghr_basedir_name/$ghr_executable_file_name" -t "$github_token" -u "$github_user" -r "$github_repository" -delete -draft "$github_tag" "$tmp_dir_name/$distribution_zipfile_name"
# ghr -t
# 3. cleanup tmp directory
rm -rf "$tmp_dir_name"