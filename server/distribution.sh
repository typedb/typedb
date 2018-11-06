#
# GRAKN.AI - THE KNOWLEDGE GRAPH
# Copyright (C) 2018 Grakn Labs Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

#!/usr/bin/env bash

set -ex # exit immediately when there's a failure. run the script in verbose mode (ie., print all the commands to stdout)

# command-line arguments
distribution_zipfile_path="$1" # get last argument
grakn_bootup_bash_script_path="$2"
grakn_logo_path="$3"
deploy_jar_path="$4"
grakn_properties_path="$5"
logback_xml_path="$6"
storage_yaml_path="$7"
storage_logback_xml_path="$8"
dashboard_assets_dir_prefix="$9"
dashboard_asset_paths="${@:10}"

# configurations
distribution_basedir_name="grakn-core-server"
confdir_name="conf"
datadir_name="db"
datadir_storagedir_name="cassandra"
datadir_queuedir_name="queue"
servicesdir_name="services"
servicesdir_grakndir_name="grakn"
servicesdir_storagedir_name="cassandra"
servicesdir_libdir_name="lib"
services_dir_assets="assets"


# 2. prepare directories and files
mkdir -p "$distribution_basedir_name"
mkdir -p "$distribution_basedir_name/$confdir_name"
mkdir -p "$distribution_basedir_name/$datadir_name/$datadir_storagedir_name"
mkdir -p "$distribution_basedir_name/$datadir_name/$datadir_queuedir_name"
mkdir -p "$distribution_basedir_name/$servicesdir_name/$servicesdir_libdir_name"
mkdir -p "$distribution_basedir_name/$servicesdir_name/$servicesdir_grakndir_name"
mkdir -p "$distribution_basedir_name/$servicesdir_name/$servicesdir_storagedir_name"
mkdir -p "$distribution_basedir_name/$servicesdir_name/$services_dir_assets"

# 3. copying files into the respective locations
cp "$grakn_bootup_bash_script_path" "$distribution_basedir_name"
cp "$grakn_properties_path" "$distribution_basedir_name/$confdir_name"
cp "$logback_xml_path" "$distribution_basedir_name/$confdir_name"
cp "$grakn_logo_path" "$distribution_basedir_name/$servicesdir_name/$servicesdir_grakndir_name"
cp "$storage_yaml_path" "$distribution_basedir_name/$servicesdir_name/$servicesdir_storagedir_name"
cp "$storage_logback_xml_path" "$distribution_basedir_name/$servicesdir_name/$servicesdir_storagedir_name"
cp "$deploy_jar_path" "$distribution_basedir_name/$servicesdir_name/$servicesdir_libdir_name"
for asset in $dashboard_asset_paths; do
    asset_target_dir=$(dirname "$distribution_basedir_name/$servicesdir_name/$services_dir_assets/${asset#$dashboard_assets_dir_prefix}")
    mkdir -p "$asset_target_dir" && cp "${asset}" "$asset_target_dir"
done

# 4. the grakn-core-server distribution will contain the following files
find $distribution_basedir_name

# 5. building zip
zip -r "$distribution_zipfile_path" "$distribution_basedir_name"

# 6. cleanup
rm -rf "$distribution_basedir_name"
