# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_test")

exports_files(
    [
        "schema.tql",
        "data.typedb",
    ],
)

host_compatible_java_test(
    name = "test-migrator",
    size = "large",
    srcs = ["MigratorTest.java"],
    data = [
        "data.typedb",
        ":schema.tql",
    ],
    native_libraries_deps = [
        "//:typedb",
        "//common:common",
        "//server:server",
        "//database:database",
        "//migrator:migrator",
    ],
    test_class = "com.vaticle.typedb.core.migrator.MigratorTest",
    deps = [
        # Internal dependencies
        "//test/integration/util",
        "//migrator:migrator-data",
        "//server:version",

        # External dependencies from Maven
        "@maven//:com_google_protobuf_protobuf_java",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
    ]),
    exclude = ["data.typedb"],
    license_type = "mpl-header",
)
