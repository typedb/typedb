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
#
package(default_visibility = ["//visibility:public"])

load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

java_library(
    name = "reasoner",
    srcs = glob([
        "**/*.java",
    ]),
    tags = ["maven_coordinates=io.grakn.core:grakn-graql-reasoner:{pom_version}"],
    deps = [
        # Internal dependencies
        "//common",
        "//kb/concept/api",
        "//kb/concept/manager",
        "//kb/graql/reasoner",
        "//kb/graql/executor",
        "//kb/graql/exception",
        "//kb/graql/planning",
        "//kb/keyspace",
        "//concept/answer",
        "//concept/util",
        "//graql/planning",
        "//core",

        # External dependencies from @graknlabs
        "@graknlabs_graql//java:graql",
        "@graknlabs_common//:common",

        # External dependencies from Maven
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_guava",
        "@maven//:commons_cli_commons_cli",
        "@maven//:commons_configuration_commons_configuration",
        "@maven//:commons_lang_commons_lang",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_stub",
        "@maven//:io_grpc_grpc_api",
        "@maven//:org_apache_cassandra_cassandra_all",
        "@maven//:org_apache_thrift_libthrift",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_apache_hadoop_hadoop_common",
        "@maven//:org_apache_hadoop_hadoop_mapreduce_client_core",
        "@maven//:org_apache_spark_spark_core_2_11",
        "@maven//:org_apache_spark_spark_launcher_2_11",
        "@maven//:org_apache_tinkerpop_gremlin_core",
        "@maven//:org_apache_tinkerpop_hadoop_gremlin",
        "@maven//:org_apache_tinkerpop_spark_gremlin",
        "@maven//:org_apache_tinkerpop_tinkergraph_gremlin",
        "@maven//:org_scala_lang_scala_library",
        "@maven//:org_slf4j_slf4j_api",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":reasoner",
    ],
)
