#!/usr/bin/env bash

# inputs
script="$1"
jar="$2"
logback_xml="$3"
output="$4"

# configurations
base_dir="console"
jar_dir="."
conf_dir="conf"

# prepare directories and files
mkdir "$base_dir"
mkdir "$base_dir/$jar_dir"
mkdir "$base_dir/$conf_dir"
cp "$script" "$base_dir"
cp "$jar" "$base_dir/$jar_dir"
cp "$logback_xml" "$base_dir/$logback_xml"

zip -r "$output" "$base_dir"

rm -r "$base_dir"