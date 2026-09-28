# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

set -eux

echo "build-core"
rm -rf typedb-all-mac-arm64/
bazel build //:assemble-all-mac-arm64-zip
unzip bazel-bin/typedb-all-mac-arm64.zip
