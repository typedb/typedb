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
load("@graknlabs_rules_deployment//brew:rules.bzl", deploy_brew = "deploy_brew")
load("@graknlabs_rules_deployment//distribution:rules.bzl", distribution = "distribution")


py_binary(
    name = "deploy-github-zip",
    srcs = ["@graknlabs_rules_deployment//github:deployment.py"],
    data = [":distribution", ":VERSION", ":deployment.properties", "@ghr_osx_zip//file:file", "@ghr_linux_tar//file:file"],
    main = "deployment.py"
)

distribution(
    name = "distribution",
    targets = ["//server:server-binary", "//console:console-binary"],
    additional_files = {
         "//:grakn": 'grakn',
         "//server:conf/logback.xml": "conf/logback.xml",
         "//server:conf/grakn.properties": "conf/grakn.properties",
         "//server:services/cassandra/cassandra.yaml": "services/cassandra/cassandra.yaml",
         "//server:services/cassandra/logback.xml": "services/cassandra/logback.xml",
         "//server:services/grakn/grakn-core-ascii.txt": "services/grakn/grakn-core-ascii.txt"
    },
    empty_directories = [
        "db/cassandra",
        "db/queue"
    ],
    output_filename = "grakn-core-all",
    visibility = ["//visibility:public"]
)

deploy_brew(
    name = "deploy-brew",
    version_file = "//:VERSION"
)
