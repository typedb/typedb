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

def _apt_install_command_impl(ctx):
    fn = "{}_apt_install.py".format(ctx.attr.name)
    install_script = ctx.actions.declare_file(fn)

    ctx.actions.expand_template(
        template = ctx.file._apt_install_command_py,
        output = install_script,
        substitutions = {},
        is_executable = True
    )

    return DefaultInfo(
        executable = install_script,
        runfiles = ctx.runfiles(
             files=[ctx.file.workspace_refs],
             symlinks = {
                 "workspace_refs.json": ctx.file.workspace_refs
             }))

apt_install_command = rule(
    attrs = {
        "workspace_refs": attr.label(
            allow_single_file = [".json"]
        ),
        "version_file": attr.label(
            allow_single_file = True
        ),
        "_apt_install_command_py": attr.label(
            allow_single_file = True,
            default = "//test/deployment/apt/templates:apt_install_command.py"
        )
    },
    implementation = _apt_install_command_impl,
    executable = True
)
