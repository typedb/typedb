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

load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

java_library(
    name = "resolution-lib",
    srcs = [
        "Resolution.java",
        ],
    visibility = ["//visibility:public"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//test/behaviour/resolution/framework/complete",
        "//test/behaviour/resolution/framework/resolve",
        "//test/behaviour/resolution/framework/common",
        "//kb/server:server",
        "//concept/answer:answer",
        "@graknlabs_graql//java:graql",
    ],
)

java_binary(
    name = "resolution",
    main_class = "grakn.verification.resolution.framework.Resolution",
    runtime_deps = [":resolution-lib"],
)

java_binary(
    name = "resolution-debug",
    jvm_flags = [
        "-Xdebug",
        "-Xrunjdwp:transport=dt_socket,server=y,address=5005",
    ],
    main_class = "grakn.verification.resolution.framework.Resolution",
        runtime_deps = [":resolution-lib"],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":resolution-lib",
    ],
)