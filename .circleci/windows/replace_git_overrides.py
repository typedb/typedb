#!/usr/bin/env python3

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""Replace git_override() blocks in MODULE.bazel with local_path_override()."""

import re
import sys

OVERRIDES = {
    "typedb_dependencies": "../dependencies",
    "typedb_bazel_distribution": "../bazel-distribution",
    "typeql": "../typeql",
    "typedb_protocol": "../typedb-protocol",
    "typedb_behaviour": "../typedb-behaviour",
}

module_bazel = sys.argv[1] if len(sys.argv) > 1 else "MODULE.bazel"

with open(module_bazel, "r") as f:
    content = f.read()

for module, path in OVERRIDES.items():
    pattern = r'git_override\(\s*\n\s*module_name\s*=\s*"' + re.escape(module) + r'"[^)]*\)'
    replacement = (
        'local_path_override(\n'
        '    module_name = "' + module + '",\n'
        '    path = "' + path + '",\n'
        ')'
    )
    content, count = re.subn(pattern, replacement, content)
    if count == 0:
        print("WARNING: no git_override found for " + module, file=sys.stderr)
    else:
        print("Replaced git_override -> local_path_override for " + module)

with open(module_bazel, "w") as f:
    f.write(content)
