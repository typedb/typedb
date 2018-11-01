#!/usr/bin/env bash

set -x

# building zip
base="." # distribution.runfiles/grakn_core
version=`cat $base/VERSION`
root_directory_of_distribution="grakn-core-console-$version"
distribution="$root_directory_of_distribution.zip"
base_dir="$base/console/dist/grakn-core-console"

cp -RL "$base_dir" "$root_directory_of_distribution"
zip -r "$distribution" "$root_directory_of_distribution"

rm -rf "$root_directory_of_distribution"