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
#

load("@graknlabs_bazel_distribution//apt:rules.bzl", "assemble_apt", "deploy_apt")
load("@graknlabs_bazel_distribution//common:rules.bzl", "assemble_targz", "java_deps", "assemble_zip")
load("@graknlabs_bazel_distribution//rpm:rules.bzl", "assemble_rpm", "deploy_rpm")

exports_files(["grakn", "grakn.bat", "grakn-docker.sh"])

assemble_targz(
    name = "assemble-bash-targz",
    additional_files = {
        "//bin:grakn": 'grakn',
    },
    permissions = {
        "grakn": "0755",
    },
    visibility = ["//server:__pkg__", "//console:__pkg__", "//:__pkg__"]
)

assemble_targz(
    name = "assemble-bat-targz",
    additional_files = {
        "//bin:grakn.bat": 'grakn.bat',
    },
    visibility = ["//server:__pkg__", "//console:__pkg__", "//:__pkg__"]
)

assemble_apt(
    name = "assemble-apt",
    package_name = "grakn-core-bin",
    maintainer = "Grakn Labs <community@grakn.ai>",
    description = "Grakn Core (binaries)",
    version_file = "//:VERSION",
    archives = [":assemble-bash-targz"],
    installation_dir = "/opt/grakn/core/",
    empty_dirs = [
        "var/log/grakn/",
    ],
    depends = [
        "openjdk-8-jre"
    ],
    permissions = {
        "var/log/grakn/": "0777",
    },
    symlinks = {
        "usr/local/bin/grakn": "/opt/grakn/core/grakn",
        "opt/grakn/core/logs": "/var/log/grakn/",
    },
)

deploy_apt(
    name = "deploy-apt",
    target = ":assemble-apt",
    deployment_properties = "//:deployment.properties",
)

assemble_rpm(
    name = "assemble-rpm",
    package_name = "grakn-core-bin",
    installation_dir = "/opt/grakn/core/",
    version_file = "//:VERSION",
    spec_file = "//config/rpm:grakn-core-bin.spec",
    empty_dirs = [
        "var/log/grakn/",
    ],
    archives = [":assemble-bash-targz"],
    permissions = {
        "var/log/grakn/": "0777",
    },
    symlinks = {
        "usr/local/bin/grakn": "/opt/grakn/core/grakn",
        "opt/grakn/core/logs": "/var/log/grakn/",
    },
)

deploy_rpm(
    name = "deploy-rpm",
    target = ":assemble-rpm",
    deployment_properties = "//:deployment.properties",
)
