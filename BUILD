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

exports_files(["VERSION", "deployment.properties", "RELEASE_TEMPLATE.md"], visibility = ["//visibility:public"])
load("@graknlabs_bazel_distribution//apt:rules.bzl", "assemble_apt", "deploy_apt")
load("@graknlabs_bazel_distribution//brew:rules.bzl", "deploy_brew")
load("@graknlabs_bazel_distribution//common:rules.bzl", "assemble_targz", "java_deps", "assemble_zip", "checksum", "assemble_versioned")
load("@graknlabs_bazel_distribution//github:rules.bzl", "deploy_github")
load("@graknlabs_bazel_distribution//rpm:rules.bzl", "assemble_rpm", "deploy_rpm")
load("@io_bazel_rules_docker//container:image.bzl", "container_image")
load("@io_bazel_rules_docker//container:container.bzl", "container_push")

assemble_targz(
    name = "assemble-linux-targz",
    targets = ["//server:server-deps",
               "//console:console-deps",
               "//bin:assemble-bash-targz"],
    additional_files = {
        "//server:conf/logback.xml": "conf/logback.xml",
        "//server:conf/grakn.properties": "conf/grakn.properties",
        "//server:services/cassandra/cassandra.yaml": "server/services/cassandra/cassandra.yaml",
        "//server:services/cassandra/logback.xml": "server/services/cassandra/logback.xml",
        "//server:services/grakn/grakn-core-ascii.txt": "server/services/grakn/grakn-core-ascii.txt",
    },
    empty_directories = [
        "server/db/cassandra",
    ],
    permissions = {
        "server/services/cassandra/cassandra.yaml": "0777",
        "server/db/cassandra": "0777",
    },
    output_filename = "grakn-core-all-linux",
    visibility = ["//visibility:public"]
)

assemble_zip(
    name = "assemble-mac-zip",
    targets = ["//server:server-deps",
               "//console:console-deps",
               "//bin:assemble-bash-targz"],
    additional_files = {
        "//server:conf/logback.xml": "conf/logback.xml",
        "//server:conf/grakn.properties": "conf/grakn.properties",
        "//server:services/cassandra/cassandra.yaml": "server/services/cassandra/cassandra.yaml",
        "//server:services/cassandra/logback.xml": "server/services/cassandra/logback.xml",
        "//server:services/grakn/grakn-core-ascii.txt": "server/services/grakn/grakn-core-ascii.txt",
    },
    empty_directories = [
        "server/db/cassandra",
    ],
    permissions = {
        "server/services/cassandra/cassandra.yaml": "0777",
        "server/db/cassandra": "0777",
    },
    output_filename = "grakn-core-all-mac",
    visibility = ["//visibility:public"]
)

assemble_zip(
    name = "assemble-windows-zip",
    targets = ["//server:server-deps",
               "//console:console-deps",
               "//bin:assemble-bat-targz"],
    additional_files = {
        "//server:conf/logback.xml": "conf/logback.xml",
        "//server:conf/grakn.properties": "conf/grakn.properties",
        "//server:services/cassandra/cassandra.yaml": "server/services/cassandra/cassandra.yaml",
        "//server:services/cassandra/logback.xml": "server/services/cassandra/logback.xml",
        "//server:services/grakn/grakn-core-ascii.txt": "server/services/grakn/grakn-core-ascii.txt",
    },
    empty_directories = [
        "server/db/cassandra",
    ],
    permissions = {
        "server/services/cassandra/cassandra.yaml": "0777",
        "server/db/cassandra": "0777",
    },
    output_filename = "grakn-core-all-windows",
    visibility = ["//visibility:public"]
)

assemble_rpm(
    name = "assemble-linux-rpm",
    package_name = "grakn-core-all",
    version_file = "//:VERSION",
    spec_file = "//config/rpm:grakn-core-all.spec",
)


assemble_apt(
    name = "assemble-linux-apt",
    package_name = "grakn-core-all",
    maintainer = "Grakn Labs <community@grakn.ai>",
    description = "Grakn Core (all)",
    version_file = "//:VERSION",
    depends = [
        "openjdk-8-jre",
        "grakn-core-server (={version})",
        "grakn-core-console (={version})",
    ],
)

assemble_versioned(
    name = "assemble-versioned-all",
    targets = [
        ":assemble-linux-targz",
        ":assemble-mac-zip",
        ":assemble-windows-zip",
        "//console:assemble-linux-targz",
        "//console:assemble-mac-zip",
        "//console:assemble-windows-zip",
        "//server:assemble-linux-targz",
        "//server:assemble-mac-zip",
        "//server:assemble-windows-zip",
    ],
    version_file = "//:VERSION",
)

assemble_versioned(
    name = "assemble-versioned-mac",
    targets = [":assemble-mac-zip"],
    version_file = "//:VERSION"
)

checksum(
    name = "checksum-mac",
    archive = ":assemble-versioned-mac",
)

deploy_github(
    name = "deploy-github",
    deployment_properties = "//:deployment.properties",
    release_description = "//:RELEASE_TEMPLATE.md",
    archive = ":assemble-versioned-all",
    version_file = "//:VERSION"
)

deploy_brew(
    name = "deploy-brew",
    checksum = "//:checksum-mac",
    deployment_properties = "@graknlabs_build_tools//:deployment.properties",
    formula = "//config/brew:grakn-core.rb",
    version_file = "//:VERSION"
)

deploy_rpm(
    name = "deploy-rpm",
    target = ":assemble-linux-rpm",
    deployment_properties = "@graknlabs_build_tools//:deployment.properties",
)

deploy_apt(
    name = "deploy-apt",
    target = ":assemble-linux-apt",
    deployment_properties = "@graknlabs_build_tools//:deployment.properties",
)

container_image(
    name = "assemble-docker",
    base = "@openjdk_image//image",
    tars = [":assemble-linux-targz"],
    files = ["//bin:grakn-docker.sh"],
    ports = ["48555"],
    cmd = ["./grakn-docker.sh"],
    volumes = ["/server/db"]
)

container_push(
    name = "deploy-docker",
    image = ":assemble-docker",
    format = "Docker",
    registry = "index.docker.io",
    repository = "graknlabs/grakn",
    tag_file = "//:VERSION"
)
