#
# Copyright (C) 2022 Vaticle
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
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
    license_type = "agpl-header",
)
