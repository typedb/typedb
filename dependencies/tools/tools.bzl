#
# GRAKN.AI - THE KNOWLEDGE GRAPH
# Copyright (C) 2018 Grakn Labs Ltd
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


def build_tools():

    native.http_file(
        name = "buildifier",
        executable = True,
        sha256 = "d7d41def74991a34dfd2ac8a73804ff11c514c024a901f64ab07f45a3cf0cfef",
        urls = ["https://github.com/bazelbuild/buildtools/releases/download/0.11.1/buildifier"],
    )

    native.http_file(
        name = "buildifier_osx",
        executable = True,
        sha256 = "3cbd708ff77f36413cfaef89cd5790a1137da5dfc3d9b3b3ca3fac669fbc298b",
        urls = ["https://github.com/bazelbuild/buildtools/releases/download/0.11.1/buildifier.osx"],
    )

    native.http_file(
        name = "buildozer",
        executable = True,
        sha256 = "3226cfd3ac706b48fe69fc4581c6c163ba5dfa71a752a44d3efca4ae489f1105",
        urls = ["https://github.com/bazelbuild/buildtools/releases/download/0.11.1/buildozer"],
    )

    native.http_file(
        name = "buildozer_osx",
        executable = True,
        sha256 = "48109a542da2ad4bf10e7df962514a58ac19a32033e2dae8e682938ed11f4775",
        urls = ["https://github.com/bazelbuild/buildtools/releases/download/0.11.1/buildozer.osx"],
    )

    native.http_file(
        name = "unused_deps",
        executable = True,
        sha256 = "686f8943610e1a5e3d196e2209dcb35f463c3b583a056dd8ae355acdc2a089d8",
        urls = ["https://github.com/bazelbuild/buildtools/releases/download/0.11.1/unused_deps"],
    )

    native.http_file(
        name = "bazel_deps",
        executable = True,
        sha256 = "32d743d261c233c5e71d0e5fc79291c3481bbf524f95ebc73159167c3cee0d96",
        urls = ["https://github.com/graknlabs/bazel-deps/releases/download/v0.1/grakn-bazel-deps-v0.1.jar"],
    )

    native.http_archive(
        name = "rules_antlr",
        sha256 = "acd2a25f31aeeea5f58cdb434ae109d03826ae7cc11fe9efce1740102e3f4531",
        strip_prefix = "rules_antlr-0.1.0",
        urls = ["https://github.com/marcohu/rules_antlr/archive/0.1.0.tar.gz"],
    )