#!/usr/bin/env bash

# inputs
script="$1"
jar="$2"
grakn_properties="$3"
logback_xml="$4"
output="$5"

# configurations
base_dir="server"
jar_dir="."
conf_dir="conf"

# prepare directories and files
mkdir "$base_dir"
mkdir "$base_dir/$jar_dir"
mkdir "$base_dir/$conf_dir"
cp "$script" "$base_dir"
cp "$jar" "$base_dir/$jar_dir"
cp "$grakn_properties" "$base_dir/$grakn_properties"
cp "$logback_xml" "$base_dir/$logback_xml"
zip -r "$output" "$base_dir"

# cleanup
rm -r "$base_dir"