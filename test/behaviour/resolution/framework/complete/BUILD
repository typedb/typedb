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
exports_files(["completion_schema.gql"])

java_library(
    name = "complete",
    srcs = [
        "Completer.java",
        "SchemaManager.java",
    ],
    data = [
        "//test/behaviour/resolution/framework/complete:completion_schema.gql",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//concept/answer",
        "//concept:concept",
        "//:typedb",
        "//test/behaviour/resolution/framework/common",
        "@vaticle_typeql_lang_java//:typeql-lang",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":complete",
    ],
)
