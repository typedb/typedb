#
# Copyright (C) 2021 Vaticle
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

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_test")
package(default_visibility = ["//visibility:public"])


java_library(
    name = "common",
    srcs = glob(["*/*.java", "*/*/*.java"], exclude=["test/*", "*Test.java", "*/*Test.java"]),
    deps = [
        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql_lang_java//query:query",

        # External Maven Dependencies
        "@maven//:com_github_ben_manes_caffeine_caffeine",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:org_slf4j_slf4j_api",
    ],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-common:{pom_version}"],
)

java_test(
    name = "test-permutation-iterator",
    srcs = [
        "iterator/PermutationIteratorTest.java",
    ],
    test_class = "com.vaticle.typedb.core.common.iterator.PermutationIteratorTest",
    deps = [
        "//common:common",

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "agpl",
)
