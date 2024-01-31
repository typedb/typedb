#
# Copyright (C) 2022 Vaticle
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
    name = "test-parameters",
    size = "large",
    srcs = ["ParametersTest.java"],
    native_libraries_deps = [
        "//:typedb",
        "//common:common",
        "//server:server",
        "//database:database",
    ],
    data = [
        "//server:config",
    ],
    test_class = "com.vaticle.typedb.core.server.ParametersTest",
    deps = [
        "//test/integration/util:util",
        "@vaticle_typeql//common/java:common",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
    ]),
    license_type = "agpl-header",
)
