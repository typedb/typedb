#
# Copyright (C) 2021 Grakn Labs
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
load("@graknlabs_dependencies//builder/java:rules.bzl", "host_compatible_java_test")
package(default_visibility = ["//visibility:public"])


java_library(
    name = "common",
    srcs = glob(["*/*.java", "*/*/*.java"], exclude=["test/*", "*Test.java", "*/*Test.java"]),
    deps = [
        # External dependencies from Grakn Labs
        "@graknlabs_common//:common",
        "@graknlabs_graql//java/query:query",

        # External Maven Dependencies
        "@maven//:com_github_ben_manes_caffeine_caffeine",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:org_slf4j_slf4j_api",
    ],
    tags = ["maven_coordinates=io.grakn.core:grakn-common:{pom_version}"],
)

java_test(
    name = "test-permutation-iterator",
    srcs = [
        "iterator/PermutationIteratorTest.java",
    ],
    test_class = "grakn.core.common.iterator.PermutationIteratorTest",
    deps = [
        "//common:common",

        # External dependencies from Grakn Labs
        "@graknlabs_common//:common",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "agpl",
)
