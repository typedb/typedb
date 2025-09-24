set -eux

echo "build-core"
rm -rf typedb-all-mac-arm64/
bazel build //:assemble-all-mac-arm64-zip
unzip bazel-bin/typedb-all-mac-arm64.zip
