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
package(default_visibility = ["//visibility:public"])

native_java_libraries(
    name = "util",
    srcs = glob(["*.java"]),
    deps = [
        # External Maven Dependencies
        "@maven//:junit_junit",
    ],
    native_libraries_deps = [
        # Internal dependencies
        "//common:common",
    ]
)

filegroup(
    name = "logback-test",
    srcs = ["logback-test.xml"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*"]),
    license_type = "agpl",
)
