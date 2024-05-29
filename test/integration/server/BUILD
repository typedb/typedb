# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

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
    license_type = "mpl-header",
)
