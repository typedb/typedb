# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_test", "native_java_libraries")

package(default_visibility = [
    "//test/benchmark/reasoner/iam:__subpackages__",
])

filegroup(
    name = "common",
    srcs = ["types.tql", "rules.tql", "data.typedb", "params.yml"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    exclude = glob(["*.typedb"]),
    license_type = "mpl-header",
)
