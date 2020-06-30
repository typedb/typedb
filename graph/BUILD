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
    name = "graph",
    srcs = glob(
        ["**/*.java"],
        exclude = ["test/**"],
    ),
    deps = [
        # External dependencies from Maven
        "@maven//:commons_collections_commons_collections",
        "@maven//:commons_configuration_commons_configuration",
        "@maven//:commons_lang_commons_lang",
        "@maven//:com_datastax_oss_java_driver_core",
        "@maven//:com_datastax_oss_java_driver_query_builder",
        "@maven//:com_fasterxml_jackson_core_jackson_databind",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_guava",
        "@maven//:io_vavr_vavr",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_apache_tinkerpop_gremlin_core",
        "@maven//:org_apache_tinkerpop_tinkergraph_gremlin",
        "@maven//:org_javatuples_javatuples",
        "@maven//:org_slf4j_slf4j_api",
    ],
    runtime_deps = [
            "@maven//:ch_qos_logback_logback_classic",
            "@maven//:ch_qos_logback_logback_core",
    ],
    tags = ["maven_coordinates=io.grakn.core:grakn-graph:{pom_version}"],

)


checkstyle_test(
    name = "checkstyle",
    targets = [
        ":graph"
    ],
)
