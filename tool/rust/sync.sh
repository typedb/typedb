#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

SCRIPT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
ADMIN_PROTO_CARGO="$SCRIPT_DIR/server/service/admin/proto/Cargo.toml"

cp "$ADMIN_PROTO_CARGO" "$ADMIN_PROTO_CARGO.bak"
bazel run @typedb_dependencies//tool/ide:rust_sync -- @typedb_workspace_refs//:refs.json
cp "$ADMIN_PROTO_CARGO.bak" "$ADMIN_PROTO_CARGO"
rm "$ADMIN_PROTO_CARGO.bak"
