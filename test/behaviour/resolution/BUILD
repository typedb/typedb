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
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_library")

host_compatible_java_library(
    name = "steps",
    srcs = [
        "ResolutionSteps.java"
    ],
    native_libraries_deps = [
        "//:typedb",
        "//rocks:rocks",
        "//concept:concept",
        "//test/behaviour/resolution/framework:resolution",
    ],
    deps = [
        # Package dependencies
        "//common",
        "//test/behaviour/connection:steps",
        "//test/behaviour/exception",

        # External dependencies from Vaticle
        "@vaticle_typeql_lang_java//:typeql-lang",
        "@vaticle_typeql_lang_java//query",

        # External dependencies from Maven
        "@maven//:junit_junit",
        "@maven//:io_cucumber_cucumber_java",
    ],
    runtime_deps = [
        "//test/behaviour/connection/database:steps",
        "//test/behaviour/connection/transaction:steps",
    ],
    visibility = ["//visibility:public"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "agpl",
)
