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

exports_files(["grakn", "grakn-debian", "VERSION", "deployment.properties", "debian-postinst.sh"], visibility = ["//visibility:public"])
load("@graknlabs_rules_deployment//brew:rules.bzl", deploy_brew = "deploy_brew")
load("@graknlabs_rules_deployment//distribution:rules.bzl", "distribution", "deploy_deb")
load("@bazel_tools//tools/build_defs/pkg:pkg.bzl", "pkg_tar", "pkg_deb")


py_binary(
    name = "deploy-github-zip",
    srcs = ["@graknlabs_rules_deployment//github:deployment.py"],
    data = [":distribution", ":VERSION", ":deployment.properties", "@ghr_osx_zip//file:file", "@ghr_linux_tar//file:file"],
    main = "deployment.py"
)

distribution(
    targets = {
        "//server:server-binary": "server/services/lib/",
        "//console:console-binary": "console/services/lib/"
    },
    additional_files = {
        "//:grakn": 'grakn',
        "//server:conf/logback.xml": "conf/logback.xml",
        "//server:conf/grakn.properties": "conf/grakn.properties",
        "//server:services/cassandra/cassandra.yaml": "server/services/cassandra/cassandra.yaml",
        "//server:services/cassandra/logback.xml": "server/services/cassandra/logback.xml",
        "//server:services/grakn/grakn-core-ascii.txt": "server/services/grakn/grakn-core-ascii.txt"
    },
    empty_directories = [
        "server/db/cassandra",
        "server/db/queue"
    ],
    modes = {
        "server/services/cassandra/cassandra.yaml": "777",
        "server/logs": "777",
        "server/db/cassandra": "777",
        "server/db/queue": "777",
    },
    output_filename = "grakn-core-all",
)

deploy_brew(
    name = "deploy-brew",
    version_file = "//:VERSION"
)

deploy_deb(
    package_name = "grakn-core-bin",
    maintainer = "Max Vorobev <vmax0770@gmail.com>",
    description = "Grakn Core (binaries)",
    version_file = "//:VERSION",
    installation_dir = "/opt/grakn/core/",
    empty_dirs = [
        "logs",
        "/var/log/grakn/server/",
    ],
    files = {
        "//:grakn-debian": "grakn",
        "//server:conf/logback.xml": "conf/logback.xml",
        "//server:conf/grakn.properties": "conf/grakn.properties",
    },
    depends = [
      "openjdk-8-jre"
    ],
    symlinks = {
        "/usr/local/bin/grakn": "/opt/grakn/core/grakn",
        "/opt/grakn/core/logs": "/var/log/grakn/server/",
    },
)
