#!/usr/bin/env bash

set -ex # exit immediately when there's a failure. run the script in verbose mode (ie., print all the commands to stdout)

# command-line arguments
distribution_zipfile_path="$1"
grakn_bootup_bash_script_path="$2"
deploy_jar_path="$3"

# configurations
distribution_basedir_name="grakn-core-console"
distribution_servicesdir_name="services"
distribution_libdir_name="lib"

# 1. prepare directories and files
mkdir -p "$distribution_basedir_name"
mkdir -p "$distribution_basedir_name/$distribution_servicesdir_name/$distribution_libdir_name"
find $distribution_basedir_name # list the directory structures so they can be examined

# 2. copying files into the respective locations
cp "$grakn_bootup_bash_script_path" "$distribution_basedir_name"
cp "$deploy_jar_path" "$distribution_basedir_name/$distribution_servicesdir_name/$distribution_libdir_name"

# 3. the grakn-core-server distribution will contain the following files
find $distribution_basedir_name # list the files so they can be examined

# 4. building zip
zip -r "$distribution_zipfile_path" "$distribution_basedir_name"

# 5. cleanup
rm -rf "$distribution_basedir_name"