# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

package(default_visibility = ["//visibility:public"])
load("@vaticle_bazel_distribution//platform:constraints.bzl", "constraint_linux_arm64", "constraint_linux_x86_64")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

platform(
    name = "linux-x86_64",
    constraint_values = constraint_linux_x86_64,
)

platform(
    name = "linux-arm64",
    constraint_values = constraint_linux_arm64,
)

genrule(
  name = "version-arm64",
  srcs = ["//:VERSION"],
  outs = ["version-arm64.txt"],
  tools = [],
  cmd = "VERSION=$$(cat $(location //:VERSION)); echo \"$$VERSION-arm64\" > $@"
)


genrule(
  name = "version-x86_64",
  srcs = ["//:VERSION"],
  outs = ["version-x86_64.txt"],
  tools = [],
  cmd = "VERSION=$$(cat $(location //:VERSION)); echo \"$$VERSION-x86_64\" > $@"
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*",]),
    license_type = "mpl-header",
)
