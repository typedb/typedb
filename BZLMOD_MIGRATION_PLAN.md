# TypeDB: Bazel 6 to 8 Migration Plan

## Overview

Migrate `typedb` (core database server) from WORKSPACE-based builds to Bzlmod (MODULE.bazel) for Bazel 8 compatibility.

**Current Status:** Not started - uses Bazel 6.2.0 with WORKSPACE

---

## Current State

| Aspect | Status |
|--------|--------|
| Bazel Version | 6.2.0 |
| MODULE.bazel | Does not exist |
| WORKSPACE | Active, 161 lines |
| Build Status | Working on Bazel 6 |

### Key Dependencies (from WORKSPACE)

- `@typedb_dependencies` - Build tooling and shared dependencies
- `@typedb_bazel_distribution` - Packaging and deployment rules
- `@typeql` - Query language parser
- `@typedb_protocol` - gRPC protocol definitions
- `@typedb_behaviour` - BDD test specifications
- `@rules_rust` - Rust build rules
- `@rules_python` - Python build rules
- `@rules_kotlin` - Kotlin build rules
- `@rules_pkg` - Packaging rules
- `@io_bazel_rules_docker` - Docker container rules (DEPRECATED)
- `@io_bazel_rules_go` - Go rules (for Docker dependencies)
- `@bazel_gazelle` - Go dependency management
- `@crates` - Rust crate dependencies

### Main Targets

- `//:typedb_server_bin` - Rust binary (core server)
- `//server:typedb-server` - Server library
- `//database:database` - Database core
- Assembly targets for multiple platforms (macOS, Linux, Windows)
- Docker image targets
- APT/Brew deployment targets
- Checkstyle tests

### Known Challenges

1. **Docker Rules**: `@io_bazel_rules_docker` is deprecated and doesn't support Bzlmod
   - Docker targets may need to be excluded or migrated to `rules_oci`

2. **Go Rules**: Used transitively by Docker rules
   - May be unnecessary if Docker is excluded

3. **Complex Packaging**: Multiple platform-specific assembly targets

---

## Migration Steps

### Step 1: Create MODULE.bazel

Create `/opt/project/repositories/typedb/MODULE.bazel` with:

```python
module(
    name = "typedb",
    version = "0.0.0",
    compatibility_level = 1,
)

# Core BCR dependencies
bazel_dep(name = "bazel_skylib", version = "1.7.1")
bazel_dep(name = "rules_java", version = "8.6.2")
bazel_dep(name = "rules_jvm_external", version = "6.6")
bazel_dep(name = "rules_python", version = "1.0.0")
bazel_dep(name = "rules_rust", version = "0.56.0")
bazel_dep(name = "rules_kotlin", version = "2.0.0")
bazel_dep(name = "rules_pkg", version = "1.0.1")
bazel_dep(name = "rules_proto", version = "7.1.0")
bazel_dep(name = "protobuf", version = "29.3", repo_name = "com_google_protobuf")

# typedb ecosystem - local path overrides
bazel_dep(name = "typedb_dependencies", version = "0.0.0")
local_path_override(module_name = "typedb_dependencies", path = "../dependencies")

bazel_dep(name = "typedb_bazel_distribution", version = "0.0.0")
local_path_override(module_name = "typedb_bazel_distribution", path = "../bazel-distribution")

bazel_dep(name = "typeql", version = "0.0.0")
local_path_override(module_name = "typeql", path = "../typeql")

bazel_dep(name = "typedb_protocol", version = "0.0.0")
local_path_override(module_name = "typedb_protocol", path = "../typedb-protocol")

bazel_dep(name = "typedb_behaviour", version = "0.0.0")
local_path_override(module_name = "typedb_behaviour", path = "../typedb-behaviour")

# Python toolchain
python = use_extension("@rules_python//python/extensions:python.bzl", "python")
python.toolchain(is_default = True, python_version = "3.11")

# Rust toolchain
rust = use_extension("@rules_rust//rust:extensions.bzl", "rust")
rust.toolchain(edition = "2021", versions = ["1.81.0"])
use_repo(rust, "rust_toolchains")
register_toolchains("@rust_toolchains//:all")

# Rust crates from typedb_dependencies (isolated to avoid conflicts)
crate = use_extension("@rules_rust//crate_universe:extensions.bzl", "crate", isolate = True)
crate.from_cargo(
    name = "crates",
    cargo_lockfile = "@typedb_dependencies//:library/crates/Cargo.lock",
    manifests = ["@typedb_dependencies//:library/crates/Cargo.toml"],
)
use_repo(crate, "crates")

# Kotlin toolchain
register_toolchains("@rules_kotlin//kotlin/internal:default_toolchain")
```

### Step 2: Update .bazelversion

Change from `6.2.0` to `8.0.0`.

### Step 3: Update .bazelrc

Add Bzlmod configuration:

```
# Bzlmod is now the default (Bazel 7+)
# WORKSPACE is kept for backward compatibility but is deprecated

# Enable isolated extension usages for crate universe
common --experimental_isolated_extension_usages

# Disable Docker transitions (rules_docker not compatible with Bzlmod)
build --@io_bazel_rules_docker//transitions:enable=false
```

### Step 4: Handle Docker Rules

`@io_bazel_rules_docker` does NOT support Bzlmod and is deprecated.

**Options:**
1. **Exclude Docker targets** from `//...` build (recommended for initial migration)
2. **Migrate to rules_oci** (future work)
3. **Keep Docker builds on WORKSPACE mode** with `--enable_workspace=true`

For initial migration, exclude Docker targets:
```bash
bazelisk build //... -- -//docker/...
```

### Step 5: Verify Core Targets Build

Priority targets to verify:
```bash
# Core server binary
bazelisk build //:typedb_server_bin

# Server library
bazelisk build //server:typedb-server

# Database core
bazelisk build //database:database

# Full build (excluding Docker)
bazelisk build //... -- -//docker/...
```

### Step 6: Handle Potential Issues

**Issue 1: Crate universe conflict**
- Both typedb and typedb_dependencies define crate extensions
- Solution: Use `isolate = True` and `--experimental_isolated_extension_usages`

**Issue 2: Docker rules not compatible**
- Solution: Exclude Docker targets or keep on WORKSPACE mode

**Issue 3: Go rules (transitive from Docker)**
- If Docker excluded, Go rules may not be needed

**Issue 4: typedb_console_artifact**
- External artifact downloads may need http_file configuration

### Step 7: Documentation

Create `BZLMOD_MIGRATION_STATUS.md` with:
- Build verification commands
- Excluded targets and reasons
- Known issues

---

## Dependencies Graph

```
typedb
├── typedb_dependencies (local) ✅ Bzlmod ready
│   └── typedb_bazel_distribution (local) ✅ Bzlmod ready
├── typeql (local) ✅ Bzlmod ready
├── typedb_protocol (local) ✅ Bzlmod ready
├── typedb_behaviour (local) ✅ Bzlmod ready
├── BCR modules
│   ├── rules_rust (0.56.0)
│   ├── rules_python (1.0.0)
│   ├── rules_kotlin (2.0.0)
│   ├── rules_pkg (1.0.1)
│   └── rules_proto (7.1.0)
├── Rust crates (from typedb_dependencies)
│   ├── tokio, clap, sentry
│   └── ... (many more)
└── Docker (EXCLUDED - not Bzlmod compatible)
    ├── io_bazel_rules_docker ❌
    ├── io_bazel_rules_go ❌
    └── bazel_gazelle ❌
```

---

## Verification Commands

```bash
# Full build (excluding Docker)
cd /opt/project/repositories/typedb
bazelisk build //... -- -//docker/...

# Core server only
bazelisk build //:typedb_server_bin

# Run tests (excluding Docker)
bazelisk test //... -- -//docker/...
```

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Crate universe conflict | High | Medium | Use isolate=True |
| Docker rules incompatible | Certain | Medium | Exclude Docker targets |
| Go rules issues | Medium | Low | Only needed for Docker |
| Platform assembly issues | Low | Medium | Test each platform target |
| Console artifact download | Medium | Low | Configure http_file if needed |

---

## Files to Modify

| File | Action |
|------|--------|
| `MODULE.bazel` | Create new |
| `.bazelversion` | Update 6.2.0 → 8.0.0 |
| `.bazelrc` | Add Bzlmod config, keep Docker disable flag |
| `WORKSPACE` | Keep for backward compatibility (deprecated) |
| `BZLMOD_MIGRATION_STATUS.md` | Create after migration |

---

## Docker Migration (Future Work)

After core migration is complete, Docker support can be added:

1. **Option A: rules_oci** (recommended)
   - Modern replacement for rules_docker
   - Supports Bzlmod natively
   - Requires rewriting Docker rules

2. **Option B: Hybrid mode**
   - Keep Docker builds using `--enable_workspace=true`
   - Not recommended long-term

---

## Rollback Plan

If migration fails, re-enable WORKSPACE mode:
```bash
# Add to .bazelrc
common --enable_workspace=true
common --noenable_bzlmod
```

---

## Post-Migration

After typedb is migrated, these repos can follow:
1. `typedb-driver` - depends on typedb
2. `typedb-console` - depends on typeql, typedb-driver
3. `typedb-cluster` - depends on typedb
4. `typedb-studio` - depends on driver
