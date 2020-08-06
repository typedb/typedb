#
# Copyright (C) 2020 Grakn Labs
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

package(default_visibility = ["//visibility:__pkg__"])

load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

java_library(
    name = "daemon",
    srcs = glob(["**/*.java"]),
    deps = [

        # Grakn Core dependencies
        "//common:common",
        "//server:server",
        "//server/migrate/proto:migrate-proto-java",

        # External dependencies
        "@maven//:org_slf4j_slf4j_api",
        "@maven//:com_fasterxml_jackson_core_jackson_core", # NEEDS TO GO
        "@maven//:com_fasterxml_jackson_core_jackson_databind", # NEEDS TO GO
        "@maven//:com_fasterxml_jackson_dataformat_jackson_dataformat_yaml", # NEEDS TO GO
        "@maven//:commons_io_commons_io",
        "@maven//:org_apache_cassandra_cassandra_all",
        "@maven//:org_zeroturnaround_zt_exec",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_stub",
        "@maven//:com_google_protobuf_protobuf_java",
    ],
    runtime_deps = [
        "@maven//:ch_qos_logback_logback_classic",
        "@maven//:ch_qos_logback_logback_core",
    ],
    resources = ["LICENSE"],
    resource_strip_prefix = "daemon",
    visibility = ["//visibility:public"],
    tags = ["maven_coordinates=io.grakn.core:grakn-daemon:{pom_version}"],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":daemon"
    ],
)
