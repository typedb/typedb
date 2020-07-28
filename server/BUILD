#
# Copyright (C) 2019 Grakn Labs
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

load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
package(default_visibility = ["//visibility:private",])

genrule(
    name = "version",
    srcs = [
        "templates/Version.java",
        "//:VERSION",
    ],
    cmd = "VERSION=`cat $(location //:VERSION)`;sed -e \"s/{version}/$$VERSION/g\" $(location templates/Version.java) >> $@",
    outs = ["Version.java"],
    visibility = ["//common:__subpackages__"],
)

java_library(
    name = "server",
    srcs = glob(["*.java", "*/*.java", "*/*/*.java"], exclude=["templates/**", "test/**"]) + [":version"],
    deps = [
        # Internal package dependencies
        "//:grakn",
        "//common:common",
        # "//concept:concept",
        "//rocks:rocks",

        # External dependencies from @graknlabs
        "@graknlabs_common//:common",
        "@graknlabs_grabl_tracing//client:client",
        "@graknlabs_protocol//grpc/java:protocol",

        # External dependencies
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:info_picocli_picocli",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_stub",
        "@maven//:io_netty_netty_all",
        "@maven//:org_slf4j_slf4j_api",
    ],
    runtime_deps = [
        "@maven//:ch_qos_logback_logback_classic",
    ],
    tags = ["maven_coordinates=grakn.core:grakn-core-server:{pom_version}"],
    visibility = [ "//visibility:public" ]
)

java_binary(
    name = "start",
    main_class = "grakn.core.server.GraknServer",
    runtime_deps = [":server"],
    data = [
        "//server/conf:grakn.properties",
        "//server/resources:logo",
    ],
    classpath_resources = ["//server/conf:logback"],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":server",
    ]
)
