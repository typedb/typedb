#!/usr/bin/env bash

set -x

# arguments
github_user="lolski"
github_token="ecc71a66996c225b5b3759e85b508d96b3e8f885"
github_repository="$1"
distribution_name="$2"
a="$3"

# configurations
version=`cat VERSION`
distribution_basedir_dirname="$distribution_name-$version"
distribution_zip_filename="$distribution_basedir_dirname.zip"
github_tag="v$version" # e.g, v1.5.0
ghr_basename="ghr_v0.10.2_darwin_386"
ghr_base="external/ghr/file"
ghr_zip="$ghr_base/$ghr_basename.zip"
tmp_directory="tmp"

# 0. initialise tmp directory
mkdir "$tmp_directory"
cp "$a" "$tmp_directory/$distribution_zip_filename"

# 2. create a draft release
unzip "$ghr_zip" -d "$tmp_directory/"
"$tmp_directory/$ghr_basename/ghr" -t "$github_token" -u "$github_user" -r "$github_repository" -delete -draft "$github_tag" "$tmp_directory/$distribution_zip_filename"

# 3. cleanup tmp directory
rm -rf "$tmp_directory"