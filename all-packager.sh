#!/usr/bin/env bash

echo "packaging the grakn-core-all distribution..."
# inputs
output="$1"
script="$2"
console_jar="$3"
console_logback_xml="$4"
server_jar="$5"
server_grakn_properties="$6"
server_logback_xml="$7"
server_storage_yaml="$8"
server_storage_logback_xml="$9"
server_dashboard_assets_dir_prefix="$10"
server_dashboard_assets="${@:11}"

# configurations
base_dir="grakn-core-all"
conf_dir="conf"
services_dir="services"
services_dir_lib="lib"
server_data_dir="db"
server_data_dir_storage="cassandra"
server_data_dir_queue="queue"
server_services_dir="services"
server_services_dir_storage="cassandra"
server_services_dir_assets="assets"

# prepare directories and files
echo "making directories..."
mkdir -p "$base_dir"
mkdir -p "$base_dir/$conf_dir"
mkdir -p "$base_dir/$services_dir/$services_dir_lib"
mkdir -p "$base_dir/$server_data_dir/$server_data_dir_storage"
mkdir -p "$base_dir/$server_data_dir/$server_data_dir_queue"
mkdir -p "$base_dir/$server_services_dir/$server_data_dir_storage"
mkdir -p "$base_dir/$server_services_dir/$server_services_dir_assets"
echo "the following directories have been created:"
find $base_dir

echo "copying files into the respective locations..."
cp "$script" "$base_dir"
#cp "$console_logback_xml" "$base_dir/$conf_dir"
cp "$console_jar" "$base_dir/$services_dir/$services_dir_lib"
cp "$server_grakn_properties" "$base_dir/$conf_dir"
cp "$server_logback_xml" "$base_dir/$conf_dir"
cp "$server_storage_yaml" "$base_dir/$server_services_dir/$server_services_dir_storage"
cp "$server_storage_logback_xml" "$base_dir/$server_services_dir/$server_services_dir_storage"
cp "$server_jar" "$base_dir/$server_services_dir/$services_dir_lib"
for asset in $server_dashboard_assets; do
    asset_target_dir=$(dirname "$base_dir/$server_services_dir/$server_services_dir_assets/${asset#$server_dashboard_assets_dir_prefix}")
    echo "$asset" "$asset_target_dir"
    mkdir -p "$asset_target_dir" && cp "${asset}" "$asset_target_dir"
done

echo "the grakn-core-all distribution will contain the following files:"
find $base_dir

zip -r "$output" "$base_dir"

echo "the grakn-core-all distribution has been successfully created!"

# cleanup
rm -r "$base_dir"