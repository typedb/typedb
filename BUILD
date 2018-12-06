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

sh_binary(
    name = "deploy-github-zip",
    srcs = ["@graknlabs_rules_deployment//github:deployment.sh"],
    data = [":distribution", ":VERSION", ":deployment.properties", "@ghr_osx_zip//file:file", "@ghr_linux_tar//file:file"]
)

genrule(
    name = "distribution",
    srcs = [
        "//:grakn",
        "//console:console-binary_deploy.jar",
        "//server:conf/logback.xml",
        "//server:conf/grakn.properties",
        "//server:server-binary_deploy.jar",
        "//server:services/cassandra/cassandra.yaml",
        "//server:services/cassandra/logback.xml",
        "//server:services/grakn/grakn-core-ascii.txt",
    ],
    outs = ["//:dist/grakn-core-all.zip"],
    cmd = "$(location distribution.sh) $(location //:dist/grakn-core-all.zip) $(location //:grakn) $(location //server:services/grakn/grakn-core-ascii.txt) $(location //console:console-binary_deploy.jar) $(location //server:server-binary_deploy.jar) $(location //server:conf/grakn.properties) $(location //server:conf/logback.xml) $(location //server:services/cassandra/logback.xml) $(location //server:services/cassandra/cassandra.yaml)",
    tools = ["distribution.sh"],
    visibility = ["//visibility:public"]
)

deploy_brew(
    name = "deploy-brew",
    version_file = "//:VERSION"
)
