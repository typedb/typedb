#!/usr/bin/env bash

set -x

# 1. modules and configurations
# inputs
output="$1"
script="$2"
jar="$3"

# configurations
base_dir="$output"
services_dir="services"
services_dir_lib="lib"

# 2. prepare directories and files
mkdir -p "$base_dir"
mkdir -p "$base_dir/$services_dir/$services_dir_lib"
find $base_dir

# 3. copying files into the respective locations
cp "$script" "$base_dir"
cp "$jar" "$base_dir/$services_dir/$services_dir_lib"

# 4. the grakn-core-server distribution will contain the following files
find $base_dir