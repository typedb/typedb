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
exports_files(["completion_schema.gql"])

java_library(
    name = "complete",
    srcs = [
        "Completer.java",
        "SchemaManager.java",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//test/behaviour/resolution/framework/common:common",
        "//test/behaviour/resolution/framework/resolve:resolve",
        "//kb/server:server",
        "//kb/concept/api:api",
        "//concept/answer:answer",
        "@graknlabs_graql//java:graql",
    ],
    data = [
        "//test/behaviour/resolution/framework/complete:completion_schema.gql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":complete",
    ],
)
