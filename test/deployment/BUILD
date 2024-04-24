# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "apt",
    test_class = "com.vaticle.typedb.core.test.deployment.AptTest",
    srcs = ["AptTest.java"],
    deps = [
        "@maven//:org_slf4j_slf4j_api",
        "@maven//:org_zeroturnaround_zt_exec",
        "@maven//:com_eclipsesource_minimal_json_minimal_json",
    ],
    runtime_deps = [
        "@maven//:ch_qos_logback_logback_classic",
    ],
    data = ["@vaticle_typedb_workspace_refs//:refs.json", "//:VERSION"],
    tags = ["no-sandbox"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)
