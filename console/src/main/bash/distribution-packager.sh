#!/usr/bin/env bash

set -e # exit immediately when there's a failure. add verbose flag '-x', (ie., 'set -xe') in order to debug the script

echo "========================================="
echo "packaging the grakn-core-all distribution"
echo "========================================="
# 1. modules and configurations
# inputs
output="$1"
script="$2"
jar="$3"

# configurations
base_dir="grakn-core-console"
services_dir="services"
services_dir_lib="lib"
echo

# 2. prepare directories and files
echo "--- making directories ---"
mkdir -p "$base_dir"
mkdir -p "$base_dir/$services_dir/$services_dir_lib"
echo "--- the following directories have been created ---"
find $base_dir
echo

# 3. copying files into the respective locations
echo "--- copying files into the respective locations ---"
cp "$script" "$base_dir"
cp "$jar" "$base_dir/$services_dir/$services_dir_lib"
echo

# 4. the grakn-core-server distribution will contain the following files
echo "--- the grakn-core-console distribution will contain the following files ---"
find $base_dir
echo

# 5. building zip
echo "--- building zip: 'zip -r \"$output\" \"$base_dir\"' ---"
zip -r "$output" "$base_dir"
echo

echo "====================================================="
echo "the grakn-core-console distribution has been created!"
echo "====================================================="

# 6. cleanup
rm -r "$base_dir"