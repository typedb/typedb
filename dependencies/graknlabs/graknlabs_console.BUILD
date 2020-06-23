load("@bazel_tools//tools/build_defs/pkg:pkg.bzl", "pkg_tar")

pkg_tar(
    name = "console",
    extension = "tgz",
    srcs = glob([
        "console/**/*",
    ]),
    strip_prefix = ".",
    visibility = ["//visibility:public"],
)