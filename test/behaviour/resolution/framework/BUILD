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

native_java_libraries(
    name = "resolution-lib",
    srcs = [
        "CorrectnessChecker.java",
        ],
    visibility = ["//visibility:public"],
    native_libraries_deps = [
        "//:typedb",
        "//rocks:rocks",
        "//concept:concept",
        "//test/behaviour/resolution/framework/reference:reference",
        "//test/behaviour/resolution/framework/soundness:soundness",
    ],
    deps = [
        "//common",
        "@maven//:com_google_guava_guava",
        "@vaticle_typeql_lang_java//:typeql-lang",
        "@vaticle_typeql_lang_java//query",
    ],
)

java_binary(
    name = "resolution",
    main_class = "grakn.verification.resolution.framework.CorrectnessChecker",
    runtime_deps = [":resolution-lib"],
)

java_binary(
    name = "resolution-debug",
    jvm_flags = [
        "-Xdebug",
        "-Xrunjdwp:transport=dt_socket,server=y,address=5005",
    ],
    main_class = "grakn.verification.resolution.framework.CorrectnessChecker",
        runtime_deps = [":resolution-lib"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "agpl",
)