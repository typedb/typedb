# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_test")

host_compatible_java_test(
    name = "test-basic",
    srcs = ["BasicTest.java"],
    test_class = "com.vaticle.typedb.core.test.integration.BasicTest",
    native_libraries_deps = [
        # Internal dependencies
        "//:typedb",
        "//common:common",
        "//database:database",
        "//concept:concept",
        "//logic:logic",
    ],
    deps = [
        "//test/integration/util:util",
        # External dependencies from Vaticle
        "@vaticle_typeql//java/pattern:pattern",
        "@vaticle_typeql//java:typeql-lang",
    ],
    size = "large",
)

host_compatible_java_test(
    name = "test-query",
    srcs = ["QueryTest.java"],
    test_class = "com.vaticle.typedb.core.test.integration.QueryTest",
    native_libraries_deps = [
        "//:typedb",
        "//database:database",
        "//common:common",
        "//concept:concept",
    ],
    deps = [
        # Internal dependencies
        "//test/integration/util:util",

        # External dependencies from Vaticle
        "@vaticle_typeql//java/query:query",
        "@vaticle_typeql//java:typeql-lang",
    ],
    data = [":schema.tql"],
)

host_compatible_java_test(
    name = "test-attribute-string",
    srcs = ["StringAttributeTest.java"],
    test_class = "com.vaticle.typedb.core.test.integration.StringAttributeTest",
    native_libraries_deps = [
        # Internal dependencies
        "//:typedb",
        "//common:common",
        "//database:database",
        "//concept:concept",
        "//logic:logic",
    ],
    deps = [
        "//test/integration/util:util",

        # External dependencies from Vaticle
        "@vaticle_typeql//common/java:common",
        "@vaticle_typeql//java:typeql-lang",
    ],
    size = "large",
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
    ]),
    license_type = "mpl-header",
)
