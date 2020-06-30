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
    name = "planning",
    srcs = glob([
        "**/*.java",
    ]),
    tags = ["maven_coordinates=io.grakn.core:grakn-graql-planning:{pom_version}"],
    deps = [
        # Internal dependencies
        "//common",
        "//kb/concept/api",
        "//kb/concept/manager",
        "//kb/graql/executor",
        "//kb/graql/planning",
        "//kb/keyspace",
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
        "@maven//:io_grpc_grpc-core",
        "@maven//:io_grpc_grpc-stub",
        "@maven//:io_grpc_grpc-api",
        "@maven//:org_apache_cassandra_cassandra-all",
        "@maven//:org_apache_thrift_libthrift",
        "@maven//:org_apache_commons_commons-lang3",
        "@maven//:org_apache_hadoop_hadoop-common",
        "@maven//:org_apache_hadoop_hadoop-mapreduce-client-core",
        "@maven//:org_apache_spark_spark-core-2-11",
        "@maven//:org_apache_spark_spark-launcher-2-11",
        "@maven//:org_apache_tinkerpop_gremlin-core",
        "@maven//:org_apache_tinkerpop_hadoop-gremlin",
        "@maven//:org_apache_tinkerpop_spark-gremlin",
        "@maven//:org_apache_tinkerpop_tinkergraph-gremlin",
        "@maven//:org_scala-lang_scala-library",
        "@maven//:org_slf4j_slf4j-api",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":planning",
    ],
)
