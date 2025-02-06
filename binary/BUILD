# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#

load("@typedb_bazel_distribution//apt:rules.bzl", "assemble_apt", "deploy_apt")
load("@typedb_bazel_distribution//common:rules.bzl", "assemble_targz")
load("@typedb_dependencies//distribution:deployment.bzl", "deployment")
load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

exports_files(["typedb", "typedb.bat", "typedb.service"])
checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)
