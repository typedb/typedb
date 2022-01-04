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

package(default_visibility = ["//test/behaviour:__subpackages__"])
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_library")

host_compatible_java_library(
    name = "steps",
    srcs = [
        "TransactionSteps.java"
    ],
    deps = [
        "//test/behaviour/connection:steps",

        # Internal Repository Dependencies
        "@vaticle_typedb_common//:common",

        # External Maven Dependencies
        "@maven//:junit_junit",
        "@maven//:io_cucumber_cucumber_java",
    ],
    native_libraries_deps = [
        # Internal dependencies
        "//:typedb",
        "//common:common",
        "//common/test:util",
    ],
)

java_test(
    name = "test",
    srcs = [
        "TransactionTest.java",
    ],
    test_class = "com.vaticle.typedb.core.test.behaviour.connection.transaction.TransactionTest",
    deps = [
        # External Maven Dependencies
        "@maven//:io_cucumber_cucumber_junit",
    ],
    runtime_deps = [
        ":steps",
        "//test/behaviour/config:parameters",
        "//test/behaviour/connection/database:steps",
        "//test/behaviour/connection/session:steps",
        "//test/behaviour/typeql:steps",
    ],
    data = [
        "@vaticle_typedb_behaviour//connection:transaction.feature",
    ],
    size = "small",

)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "agpl",
)
