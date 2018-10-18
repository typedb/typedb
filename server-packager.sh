#!/usr/bin/env bash

set -e

echo "packaging the grakn-core-server distribution..."
# inputs
output="$1" # get last argument
script="$2"
jar="$3"
grakn_properties="$4"
logback_xml="$5"
storage_yaml="$6"
storage_logback_xml="$7"
dashboard_assets_dir_prefix="$8"
dashboard_assets="${@:9}"

# configurations
base_dir="grakn-core-server"
conf_dir="conf"

data_dir="db"
data_dir_storage="cassandra"
data_dir_queue="queue"

services_dir="services"
services_dir_storage="cassandra"
services_dir_lib="lib"
services_dir_assets="assets"

# prepare directories and files

echo "making directories..."
mkdir -p "$base_dir"
mkdir -p "$base_dir/$conf_dir"
mkdir -p "$base_dir/$data_dir/$data_dir_storage"
mkdir -p "$base_dir/$data_dir/$data_dir_queue"
mkdir -p "$base_dir/$services_dir/$services_dir_lib"
mkdir -p "$base_dir/$services_dir/$data_dir_storage"
mkdir -p "$base_dir/$services_dir/$services_dir_assets"
echo "the following directories have been created:"
find $base_dir

echo "copying files into the respective locations..."
cp "$script" "$base_dir"
cp "$grakn_properties" "$base_dir/$conf_dir"
cp "$logback_xml" "$base_dir/$conf_dir"
cp "$storage_yaml" "$base_dir/$services_dir/$services_dir_storage"
cp "$storage_logback_xml" "$base_dir/$services_dir/$services_dir_storage"
cp "$jar" "$base_dir/$services_dir/$services_dir_lib"
for asset in $dashboard_assets; do
    asset_target_dir=$(dirname "$base_dir/$services_dir/$services_dir_assets/${asset#$dashboard_assets_dir_prefix}")
    echo "$asset" "$asset_target_dir"
    mkdir -p "$asset_target_dir" && cp "${asset}" "$asset_target_dir"
done

echo "the grakn-core-server distribution will contain the following files:"
find $base_dir

zip -r "$output" "$base_dir"

echo "the grakn-core-server distribution has been successfully created!"

# cleanup
rm -rf "$base_dir"
