# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

filegroup(
    name = "config_file",
    srcs = select({
        "//:release_mode": ["config_release.yml"],
        "//:development_mode": ["config_development.yml"],
    }),
    visibility = ["//visibility:public"],
)

genrule(
    name = "config",
    srcs = [":config_file"],
    outs = ["config.yml"],
    cmd = "cp $(SRCS) $@",
    visibility = ["//visibility:public"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)
