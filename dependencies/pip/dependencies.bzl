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

def python_dependencies():
    native.git_repository(
        name = "io_bazel_rules_python",
        remote = "https://github.com/bazelbuild/rules_python.git",
        commit = "8b5d0683a7d878b28fffe464779c8a53659fc645",
        sha256 = "8b32d2dbb0b0dca02e0410da81499eef8ff051dad167d6931a92579e3b2a1d48"
    )