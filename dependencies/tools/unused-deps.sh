#!/usr/bin/env bash

# Wrapper script for running unused_deps

platform=$(uname)

pwd

if [ "$platform" == "Darwin" ]; then
    UNUSED_DEPS_BINARY=$(pwd)/external/unused_deps_osx/file/unused_deps.osx
elif [ "$platform" == "Linux" ]; then
    UNUSED_DEPS_BINARY=$(pwd)/external/unused_deps/file/unused_deps
else
    echo "unused_deps does not have a binary for $platform"
    exit 1
fi

cd $BUILD_WORKSPACE_DIRECTORY
$UNUSED_DEPS_BINARY $*
