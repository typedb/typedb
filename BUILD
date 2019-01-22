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

exports_files(["grakn", "VERSION", "deployment.properties"], visibility = ["//visibility:public"])
load("@graknlabs_bazel_distribution//brew:rules.bzl", deploy_brew = "deploy_brew")
load("@graknlabs_bazel_distribution//distribution:rules.bzl", "distribution_structure", "distribution_zip", "distribution_deb", "distribution_rpm")
load("@graknlabs_bazel_distribution//rpm/deployment:rules.bzl", "deploy_rpm")
load("@graknlabs_bazel_distribution//deb/deployment:rules.bzl", "deploy_deb")


py_binary(
    name = "deploy-github-zip",
    srcs = ["@graknlabs_bazel_distribution//github:deploy.py"],
    data = [":distribution", ":VERSION", ":deployment.properties", "@ghr_osx_zip//file:file", "@ghr_linux_tar//file:file"],
    main = "deploy.py"
)

distribution_structure(
    name = "grakn-core-bin",
    additional_files = {
        "//:grakn": 'grakn',
        "//server:conf/logback.xml": "conf/logback.xml",
        "//server:conf/grakn.properties": "conf/grakn.properties",
    },
    visibility = ["//server:__pkg__", "//console:__pkg__"]
)


distribution_deb(
    name = "distribution-deb",
    package_name = "grakn-core-bin",
    maintainer = "Grakn Labs <community@grakn.ai>",
    description = "Grakn Core (binaries)",
    version_file = "//:VERSION",
    distribution_structures = [":grakn-core-bin"],
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

deploy_deb(
    name = "deploy-deb",
    target = ":distribution-deb",
    deployment_properties = "//:deployment.properties",
)


distribution_rpm(
    name = "distribution-rpm",
    package_name = "grakn-core-bin",
    installation_dir = "/opt/grakn/core/",
    version_file = "//:VERSION",
    spec_file = "//dependencies/distribution/rpm:grakn-core-bin.spec",
    empty_dirs = [
        "var/log/grakn/",
    ],
    distribution_structures = [":grakn-core-bin"],
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
    target = ":distribution-rpm",
    deployment_properties = "//:deployment.properties",
)

distribution_zip(
    name = "distribution",
    distribution_structures = ["//:grakn-core-bin",
                               "//server:grakn-core-server",
                               "//console:grakn-core-console"],
    empty_directories = [
        "server/db/cassandra",
        "server/db/queue"
    ],
    permissions = {
        "server/services/cassandra/cassandra.yaml": "0777",
        "server/db/cassandra": "0777",
        "server/db/queue": "0777",
    },
    output_filename = "grakn-core-all",
)


deploy_brew(
    name = "deploy-brew",
    version_file = "//:VERSION"
)
