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

host_compatible_java_test(
    name = "test-database",
    srcs = ["DatabaseTest.java"],
    test_class = "com.vaticle.typedb.core.rocks.DatabaseTest",
    native_libraries_deps = [
        "//:typedb",
        "//common:common",
        "//graph:graph",
        "//rocks:rocks",

        "//common/test:util",
    ],
    deps = [
    ],
    data = ["data"],
    size = "small",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    exclude=glob(["data/**"]),
    license_type = "agpl",
)
