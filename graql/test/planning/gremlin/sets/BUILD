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

java_test(
    name = "label-fragment-set-test",
    size = "small",
    srcs = ["LabelFragmentSetTest.java"],
    test_class = "grakn.core.graql.planning.gremlin.sets.LabelFragmentSetTest",
    deps = [
        "@maven//:com_google_guava_guava",
        "@maven//:org_apache_tinkerpop_gremlin_core",
        "@maven//:org_mockito_mockito_core",
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
    test_class = "grakn.core.graql.planning.gremlin.sets.RolePlayerFragmentSetTest",
    deps = [
        "@maven//:com_google_guava_guava",
        "@maven//:org_hamcrest_hamcrest_library",
        "@maven//:org_mockito_mockito_core",
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
