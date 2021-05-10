#
# Copyright (C) 2021 Vaticle
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

load("@vaticle_bazel_distribution//common:rules.bzl", "java_deps", "assemble_zip")

def assemble_targz(name, targets, additional_files, permissions, output_filename, **kwargs):
  assemble_zip(
      name = name + "__do_not_reference",
      targets = targets,
      additional_files = additional_files,
      permissions = permissions,
      output_filename = output_filename + "__do_not_reference",
      **kwargs
  )

  native.genrule(
      name = name,
      cmd = "unzip $(location :" + name + "__do_not_reference" + ") -d $(location :" + name + "__do_not_reference" + ")-unzipped && tar -czf $$(realpath $(OUTS)) -C $$(dirname $(location :" + name + "__do_not_reference" + "))/$$(basename $(location :" + name + "__do_not_reference" + ")-unzipped) .",
      outs = [ output_filename + ".tar.gz" ],
      srcs = [ name + "__do_not_reference" ],
      **kwargs
  )

