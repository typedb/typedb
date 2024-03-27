# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

package(default_visibility = ["//server:__subpackages__", "//:__pkg__"])

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

filegroup(
    name = "logo",
    srcs = ["typedb-ascii.txt"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    exclude = ["typedb-ascii.txt"],
    license_type = "mpl-header",
)
