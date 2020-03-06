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

java_test(
    name = "label-fragment-set-test",
    size = "small",
    srcs = ["LabelFragmentSetTest.java"],
    test_class = "grakn.core.graql.gremlin.sets.LabelFragmentSetTest",
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/maven/artifacts/org/mockito:mockito-core",
        "//graql/planning",
        "//kb/concept/api",
        "//kb/concept/manager",
        "//kb/graql/planning",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "roleplayer-fragment-set-test",
    size = "small",
    srcs = ["RolePlayerFragmentSetTest.java"],
    test_class = "grakn.core.graql.gremlin.sets.RolePlayerFragmentSetTest",
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/mockito:mockito-core",
        "//graql/planning",
        "//kb/concept/api",
        "//kb/concept/manager",
        "//kb/graql/planning",
        "@graknlabs_graql//java:graql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":label-fragment-set-test",
        ":roleplayer-fragment-set-test",
    ],
)
