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
    name = "concept-map-test",
    test_class = "grakn.core.graql.query.ConceptMapTest",
    srcs = ["ConceptMapTest.java"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/mockito:mockito-core",
        "//server"],
    size = "small"
)

java_test(
    name = "delete-query-test",
    test_class = "grakn.core.graql.query.DeleteQueryTest",
    srcs = ["DeleteQueryTest.java"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//server"],
    size = "small"
)

java_test(
    name = "insert-query-test",
    test_class = "grakn.core.graql.query.InsertQueryTest",
    srcs = ["InsertQueryTest.java"],
    deps = [
        "//server",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/mockito:mockito-core",
    ],
    size = "small"
)