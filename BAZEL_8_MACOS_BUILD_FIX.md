# Bazel 8 macOS Build Fix - Investigation Summary

**Date**: March 3, 2026
**Issue**: `bazel build --noenable_workspace //...` fails on macOS with RocksDB aligned allocation errors

## Problem Description

Build fails with errors like:
```
error: aligned allocation function of type 'void *(std::size_t, std::align_val_t)'
is only available on macOS 10.13 or newer
```

## Root Cause

1. **Dependency Upgrade Chain**:
   - March 5, 2025 (commit `39deca526`): RocksDB upgraded from 0.22.0 → 0.23.0
   - Bazel 8 migration: `crate_universe` resolved to `librocksdb-sys-0.17.3+10.4.2`
   - This bundled RocksDB C++ library version **10.4.2** (instead of 9.9.3)

2. **Technical Issue**:
   - RocksDB 10.4.2 uses C++17 aligned allocation (`operator new(size_t, std::align_val_t)`)
   - Aligned allocation is only available on macOS 10.13+
   - Bazel's auto-configured CC toolchain sets `-mmacosx-version-min=10.11`

3. **Why Configuration Fails**:
   - The auto-generated `cc_wrapper.sh` script hardcodes `-mmacosx-version-min=10.11`
   - This flag appears at the END of compiler command lines
   - In clang, the last `-mmacosx-version-min` flag wins, overriding all earlier settings
   - Example command: `clang [...] -mmacosx-version-min=10.13 [...] -mmacosx-version-min=10.11`

## Investigation - All Approaches Tested

### ❌ Failed Approach #1: Platform-Specific Config
```bash
# .bazelrc
build:darwin --cxxopt=-mmacosx-version-min=10.13
```
**Result**: Flag ignored, 10.11 still appears last

### ❌ Failed Approach #2: Environment Variables
```bash
# .bazelrc
build --action_env=MACOSX_DEPLOYMENT_TARGET=10.13
build --action_env=CXXFLAGS="-mmacosx-version-min=10.13"
```
**Result**: Environment variables don't reach the Cargo build script properly

### ❌ Failed Approach #3: Cargo Configuration
```toml
# .cargo/config.toml
[env]
CXXFLAGS = "-mmacosx-version-min=10.13"
```
**Result**: Bazel sandbox doesn't use .cargo/config.toml

### ❌ Failed Approach #4: Downgrade RocksDB Version
Changed `Cargo.toml`:
```toml
rocksdb = "0.22.0"  # Down from 0.23.0
```
**Result**: Bazel's `crate_universe` ignores Cargo.toml version specifications and still resolves to `librocksdb-sys-0.17.3+10.4.2`

### ❌ Failed Approach #5: Bazel Compiler Flags
```bash
# .bazelrc
build --copt=-mmacosx-version-min=10.13
build --linkopt=-mmacosx-version-min=10.13
```
**Result**: Flags added but still overridden by cc_wrapper.sh's 10.11

## Why All Approaches Failed

The fundamental issue is that Bazel's CC toolchain configuration generates a `cc_wrapper.sh` script with hardcoded behavior. The wrapper is located at:
```
external/rules_cc++cc_configure_extension+local_config_cc/cc_wrapper.sh
```

This wrapper always adds `-mmacosx-version-min=10.11` at the END of the command line. Since this is the last flag, it overrides any earlier configuration.

## Solution: Custom CC Toolchain

The fix requires creating a **custom CC toolchain** in the `typedb/dependencies` repository.

### Implementation Steps

1. **In `typedb/dependencies` repository**:
   - Create `toolchains/cc/BUILD.bazel`
   - Define a custom C++ toolchain configuration
   - Set `macosx_deployment_target = "10.13"`
   - Reference: https://bazel.build/tutorials/ccp-toolchain-config

2. **Register toolchain in `dependencies/MODULE.bazel`**:
   ```python
   register_toolchains("//toolchains/cc:all")
   ```

3. **Update `typedb/MODULE.bazel`**:
   - Ensure it uses the updated dependencies commit

### Alternative: LLVM Toolchain Configuration

Since `dependencies/MODULE.bazel` already uses `toolchains_llvm`, configure it properly:

```python
llvm.toolchain(
    name = "llvm_toolchain",
    llvm_version = "18.1.8",
    cxx_flags = {
        "macos": ["-mmacosx-version-min=10.13"],
    },
)
```

## Current State

- **Repository**: Clean, all changes reverted
- **Dependencies commit**: `ccdb36af9a868eeb6116d508db05a5b93e688e79`
  - No macOS-specific CC toolchain configuration found
  - Uses `toolchains_llvm` version 1.3.0 with LLVM 18.1.8
  - `.bazelrc` only has `--incompatible_strict_action_env` flags

## Files Affected

- `typedb/dependencies` repository:
  - `MODULE.bazel` - toolchain configuration
  - `toolchains/cc/BUILD.bazel` - custom toolchain (needs creation)
  - `.bazelrc` - minimal, no macOS config

## Next Steps

1. **Fix in typedb/dependencies** (recommended):
   - Create custom CC toolchain with macOS 10.13 target
   - All TypeDB projects benefit from the fix
   - Proper long-term solution

2. **Fix in typedb only** (temporary):
   - Add custom CC toolchain config to this repository
   - Works for typedb builds only
   - Quick workaround while dependencies is fixed

3. **Alternative Workaround** (not recommended):
   - Pin `librocksdb-sys` to 0.17.1 using crate annotations in MODULE.bazel
   - Stays on RocksDB 9.9.3 (works with macOS 10.11)
   - Loses features/fixes from RocksDB 10.x

## References

- Bazel CC toolchain config: https://bazel.build/tutorials/ccp-toolchain-config
- Bazel toolchain reference: https://bazel.build/docs/cc-toolchain-config-reference
- RocksDB aligned allocation issue: https://github.com/facebook/rocksdb/issues/11855
- rules_rust crate annotations: https://bazelbuild.github.io/rules_rust/crate_universe.html
- toolchains_llvm: https://github.com/bazel-contrib/toolchains_llvm

## Timeline

- **March 5, 2025**: RocksDB upgraded to 0.23.0 (commit `39deca526`)
- **Bazel 8 migration**: Dependency resolution pulled RocksDB 10.4.2
- **March 3, 2026**: Issue investigated and documented

## Contact

For questions about this fix, refer to:
- This document
- `typedb/dependencies` repository (commit `ccdb36af`)
- Bazel 8 upgrade branch: `bazel-8-upgrade`
