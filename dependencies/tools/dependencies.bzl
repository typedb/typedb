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


def tools_dependencies():

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
        sha256 = "f6e8b5b3d95709964790473eddbd9ef552f4f9bd7d4136181aaf94594ee405cb",
        urls = ["https://github.com/bazelbuild/buildtools/releases/download/0.17.2/unused_deps"],
    )

    native.http_file(
        name = "unused_deps_osx",
        executable = True,
        sha256 = "bce2a7064836d426cc59005ba75778216bda7bb06a997d58444598dac8b85ebc",
        urls = ["https://github.com/bazelbuild/buildtools/releases/download/0.17.2/unused_deps.osx"],
    )

    native.http_file(
        name = "bazel_deps",
        executable = True,
        sha256 = "43278a0042e253384543c4700021504019c1f51f3673907a1b25bb1045461c0c",
        urls = ["https://github.com/graknlabs/bazel-deps/releases/download/v0.2/grakn-bazel-deps-v0.2.jar"],
    )