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

package(default_visibility = ["//test/behaviour/graql/language/define:__subpackages__"])

load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@graknlabs_dependencies//builder/java:rules.bzl", "host_compatible_java_library")

host_compatible_java_library(
    name = "steps",
    srcs = [
        "GraqlSteps.java"
    ],
    deps = [
        # Package dependencies
        "//common",
        "//test/behaviour/connection:steps",
        "//test/behaviour/util:util",

        # External dependencies from Grakn Labs
        "@graknlabs_graql//java:graql",
        "@graknlabs_graql//java/pattern",
        "@graknlabs_graql//java/query",
        "@graknlabs_common//:common",

        # External dependencies from Maven
        "@maven//:junit_junit",
        "@maven//:io_cucumber_cucumber_java",
    ],
    native_libraries_deps = [
        "//concept:concept",
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
