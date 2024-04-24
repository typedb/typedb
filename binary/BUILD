# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#

load("@vaticle_bazel_distribution//apt:rules.bzl", "assemble_apt", "deploy_apt")
load("@vaticle_bazel_distribution//common:rules.bzl", "assemble_targz")
load("@vaticle_dependencies//distribution:deployment.bzl", "deployment")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

exports_files(["typedb", "typedb.bat", "typedb.service"])

assemble_targz(
    name = "assemble-bash-targz",
    additional_files = {
        "//binary:typedb": 'typedb',
    },
    permissions = {
        "typedb": "0755",
    },
    visibility = ["//visibility:public"]
)

assemble_targz(
    name = "assemble-apt-targz",
    additional_files = {
        "//binary:typedb.service": "typedb.service",
    },
    visibility = ["//visibility:public"]
)

assemble_targz(
    name = "assemble-bat-targz",
    additional_files = {
        "//binary:typedb.bat": "typedb.bat",
    },
    visibility = ["//visibility:public"]
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)
