#!/usr/bin/env bash

set -e # exit immediately when there's a failure. add verbose flag '-x', (ie., 'set -xe') in order to debug the script

echo "============================================"
echo "packaging the grakn-core-server distribution"
echo "============================================"
# 1. modules and configurations
# inputs
output="$1" # get last argument
script="$2"
logo="$3"
jar="$4"
grakn_properties="$5"
logback_xml="$6"
storage_yaml="$7"
storage_logback_xml="$8"
# configurations
base_dir="grakn-core-server"
conf_dir="conf"
data_dir="db"
data_dir_storage="cassandra"
data_dir_queue="queue"
services_dir="services"
services_dir_grakn="grakn"
services_dir_storage="cassandra"
services_dir_lib="lib"
echo

# 2. prepare directories and files
echo "--- making directories ---"
mkdir -p "$base_dir"
mkdir -p "$base_dir/$conf_dir"
mkdir -p "$base_dir/$data_dir/$data_dir_storage"
mkdir -p "$base_dir/$data_dir/$data_dir_queue"
mkdir -p "$base_dir/$services_dir/$services_dir_lib"
mkdir -p "$base_dir/$services_dir/$services_dir_grakn"
mkdir -p "$base_dir/$services_dir/$services_dir_storage"
echo "--- the following directories have been created ---"
find $base_dir
echo

# 3. copying files into the respective locations
echo "--- copying files into the respective locations ---"
cp "$script" "$base_dir"
cp "$grakn_properties" "$base_dir/$conf_dir"
cp "$logback_xml" "$base_dir/$conf_dir"
cp "$logo" "$base_dir/$services_dir/$services_dir_grakn"
cp "$storage_yaml" "$base_dir/$services_dir/$services_dir_storage"
cp "$storage_logback_xml" "$base_dir/$services_dir/$services_dir_storage"
cp "$jar" "$base_dir/$services_dir/$services_dir_lib"
echo

# 4. the grakn-core-server distribution will contain the following files
echo "--- the grakn-core-server distribution will contain the following files ---"
find $base_dir
echo

# 5. building zip
echo "--- building zip: 'zip -r \"$output\" \"$base_dir\"' ---"
zip -r "$output" "$base_dir"
echo

echo "================================================================"
echo "the grakn-core-server distribution has been successfully created"
echo "================================================================"

# 6. cleanup
rm -rf "$base_dir"
