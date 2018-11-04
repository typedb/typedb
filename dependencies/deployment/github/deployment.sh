#!/usr/bin/env bash

set -x

# configurations
version=`cat VERSION`
distribution_name="grakn-core-server-$version"
github_user="lolski"
github_token="ecc71a66996c225b5b3759e85b508d96b3e8f885"
github_repository="test-ghr"
github_tag="v$version" # e.g, v1.5.0
ghr_basename="ghr_v0.10.2_darwin_386"
ghr_base="external/ghr/file"
ghr_zip="$ghr_base/$ghr_basename.zip"
tmp_directory="tmp"

# 0. initialise tmp directory
mkdir "$tmp_directory"

# 1. building zip
distribution="$distribution_name.zip"
base_dir="server/dist/grakn-core-server"
cp -RL "$base_dir" "$tmp_directory/$distribution_name"
zip -r "$tmp_directory/$distribution" "$tmp_directory/$distribution_name"

# 2. create a draft release
unzip "$ghr_zip" -d "$tmp_directory/"
"$tmp_directory/$ghr_basename/ghr" -t "$github_token" -u "$github_user" -r "$github_repository" -delete -draft "$github_tag" "$tmp_directory/$distribution"

# 3. cleanup tmp directory
rm -rf "$tmp_directory"