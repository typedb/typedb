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
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_test")

package(
    default_visibility = [
        "//concept:__pkg__",
        "//logic:__subpackages__",
        "//query:__subpackages__",
        "//reasoner:__subpackages__",
        "//test/integration:__subpackages__",
        "//test/behaviour/resolution:__subpackages__",
    ]
)

native_java_libraries(
    name = "pattern",
    srcs = glob(
        ["*.java",
        "constraint/*.java",
        "constraint/thing/*.java",
        "constraint/type/*.java",
        "variable/*.java",
        "equivalence/*.java",
        ],
        exclude = ["*Test.java", "*/*Test.java"],
    ),
    deps = [
        # Internal dependencies
        "//common:common",

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_factory_tracing//client:client",
        "@vaticle_typeql_lang_java//common:common",
        "@vaticle_typeql_lang_java//pattern:pattern",

        # External dependencies from Maven
        "@maven//:com_google_code_findbugs_jsr305",
    ],
    native_libraries_deps = [
        "//traversal:traversal",
    ],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-query-pattern:{pom_version}"],
)

host_compatible_java_test(
    name = "test-alpha-equivalence",
    srcs = [
        "equivalence/AlphaEquivalenceTest.java",
    ],
    test_class = "com.vaticle.typedb.core.pattern.equivalence.AlphaEquivalenceTest",
    native_libraries_deps = [
        "//pattern:pattern",
    ],
    deps = [
        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql_lang_java//pattern",
        "@vaticle_typeql_lang_java//:typeql-lang",
    ],
)

host_compatible_java_test(
    name = "test-conjunction",
    srcs = [
        "ConjunctionTest.java",
    ],
    test_class = "com.vaticle.typedb.core.pattern.ConjunctionTest",
    native_libraries_deps = [
        "//pattern:pattern",
    ],
    deps = [
        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql_lang_java//:typeql-lang",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "agpl",
)
