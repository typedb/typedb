#
# GRAKN.AI - THE KNOWLEDGE GRAPH
# Copyright (C) 2018 Grakn Labs Ltd
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

load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
    name = "session-it",
    test_class = "grakn.core.server.session.SessionIT",
    srcs = ["SessionIT.java"],
    deps = ["//server", "//test-integration/rule:grakn-test-server"],
    classpath_resources = ["//test-integration/resources:logback-test"]
)

java_test(
    name = "session-cache-it",
    test_class = "grakn.core.server.session.SessionCacheIT",
    srcs = ["SessionCacheIT.java"],
    deps = ["//server", "//test-integration/rule:grakn-test-server", "//client-java"],
    classpath_resources = ["//test-integration/resources:logback-test"]
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":session-it",
        ":session-cache-it"
    ],
)
