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

load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")
package(
    default_visibility = [
        "//core:__pkg__",
        "//cluster:__pkg__",
        "//concept:__pkg__",
        "//reader:__pkg__",
        "//writer:__pkg__"
    ]
)

java_library(
    name = "graph",
    srcs = glob(["*.java"]),
    deps = [
        "//common:common",
        "//operation:operation",
    ]
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":graph",
    ]
)