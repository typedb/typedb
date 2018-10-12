#!/usr/bin/env bash

# inputs
script="$1"
jar="$2"
grakn_properties="$3"
logback_xml="$4"
storage_yaml="$5"
storage_logback_xml="$6"
output="$7"

# configurations
base_dir="grakn-core-server"
conf_dir="conf"

data_dir="db"
data_dir_storage="cassandra"
data_dir_queue="queue"

services_dir="services"
services_dir_storage="cassandra"
services_dir_lib="lib"

# prepare directories and files
mkdir -p "$base_dir"
mkdir -p "$base_dir/$conf_dir"
mkdir -p "$base_dir/$data_dir/$data_dir_storage"
mkdir -p "$base_dir/$data_dir/$data_dir_queue"
mkdir -p "$base_dir/$services_dir/$services_dir_lib"
mkdir -p "$base_dir/$services_dir/$data_dir_storage"

cp "$script" "$base_dir"
cp "$grakn_properties" "$base_dir/$conf_dir"
cp "$logback_xml" "$base_dir/$conf_dir"
cp "$storage_yaml" "$base_dir/$services_dir/$services_dir_storage"
cp "$storage_logback_xml" "$base_dir/$services_dir/$services_dir_storage"
cp "$jar" "$base_dir/$services_dir/$services_dir_lib"

zip -r "$output" "$base_dir"

# cleanup
rm -rf "$base_dir"