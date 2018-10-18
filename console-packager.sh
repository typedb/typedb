#!/usr/bin/env bash

set -e

echo "packaging the grakn-core-all distribution..."
# inputs
output="$1"
script="$2"
jar="$3"
logback_xml="$4"

# configurations
base_dir="grakn-core-console"
conf_dir="conf"
services_dir="services"
services_dir_lib="lib"

# prepare directories and files
echo "making directories..."
mkdir -p "$base_dir"
mkdir -p "$base_dir/$conf_dir"
mkdir -p "$base_dir/$services_dir/$services_dir_lib"
echo "the following directories have been created:"
find $base_dir

cp "$script" "$base_dir"
cp "$logback_xml" "$base_dir/$conf_dir"
cp "$jar" "$base_dir/$services_dir/$services_dir_lib"

echo "the grakn-core-console distribution will contain the following files:"
find $base_dir

zip -r "$output" "$base_dir"

echo "the grakn-core-console distribution has been successfully created!"

# cleanup
rm -r "$base_dir"