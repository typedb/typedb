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

java_test(
    name = "attribute-deduplicator-it",
    test_class = "grakn.core.server.AttributeDeduplicatorIT",
    srcs = ["AttributeDeduplicatorIT.java"],
    deps = [
        "//server",
        "//test-integration/rule:grakn-test-server",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library"],
    size = "large"
)

load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")
checkstyle_test(
 name = "attribute-deduplicator-it-checkstyle",
 target = ":attribute-deduplicator-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

)