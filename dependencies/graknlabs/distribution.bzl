#
# Copyright (C) 2020 Grakn Labs
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

load("@graknlabs_bazel_distribution//distribution:rules.bzl", "distribution_file")

def graknlabs_console_distribution():
    distribution_file(
        name = "graknlabs_console_distribution",
        repository_url = "https://repo.grakn.ai/repository",
        group_name = "graknlabs_console",
        artifact_name = "console-distribution",
        extension = "tgz",
        commit = "48b1abdece294fc0640702a0a467024c313e3e47",
    )