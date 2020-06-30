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
    name = "mapreduce",
    srcs = glob(
        ["**/*.java"],
        exclude = ["test/**"],
    ),
    deps = [
        # Internal dependencies
        "//graph",
        "//common",
        # External dependencies from @graknlabs

        # External dependencies from Maven
        "@maven//:org_slf4j_slf4j-api",
        "@maven//:com_google_guava_guava",
        "@maven//:org_apache_hadoop_hadoop-mapreduce-client-core",
        "@maven//:org_apache_hadoop_hadoop-common",
        "@maven//:org_apache_hadoop_hadoop-annotations",
        "@maven//:org_apache_tinkerpop_hadoop-gremlin",
        "@maven//:org_apache_tinkerpop_gremlin-core",
        "@maven//:org_apache_tinkerpop_tinkergraph-gremlin",
        "@maven//:commons_configuration_commons_configuration",
        "@maven//:org_apache_commons_commons-lang3",
        "@maven//:org_apache_cassandra_cassandra-all",
        "@maven//:com_datastax_oss_java-driver-core",

#        The following 2 dependencies should be removed once GraknInputFormat and InputFormatCQL stop depending on old Thrift trickery
        "@maven//:org_apache_cassandra_cassandra-thrift",
        "@maven//:org_apache_thrift_libthrift"
    ],
    tags = ["maven_coordinates=io.grakn.core:grakn-graph-hadoop:{pom_version}"],

)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":mapreduce"
    ]
)
