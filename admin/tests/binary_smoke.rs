/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// Spawns the cargo-built typedb_server_bin and typedb_admin_bin and asserts the
// admin tool can connect via the per-OS admin endpoint and round-trip a command.
//
// Complementary to:
//   * //server:test_admin_service          -- library / in-process, all OSes
//   * //tests/assembly:test_admin_assembly -- archive + bash launcher, Linux/Mac
// This test covers what only-the-binary-cares-about: clap parsing of the
// per-OS socket path, tokio runtime startup from main, exit-code propagation.
//
// On Windows we drive this via cargo from .circleci/windows/test_admin.bat
// because rules_rust 0.56's crate_universe leaves ring's MSVC headers
// dangling on Windows; on Unix the assembly test covers the binaries via
// bazel, so this test is currently only run in Windows CI.

use std::{
    env,
    path::PathBuf,
    process::{Child, Command},
    thread,
    time::Duration,
};

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).parent().unwrap().to_path_buf()
}

fn bin_path(name: &str) -> PathBuf {
    let exe = if cfg!(windows) { format!("{name}.exe") } else { name.to_string() };
    workspace_root().join("target").join("debug").join(exe)
}

#[cfg(unix)]
fn admin_endpoint() -> String {
    env::temp_dir().join(format!("typedb-admin-bin-smoke-{}.sock", std::process::id())).to_string_lossy().into_owned()
}

#[cfg(windows)]
fn admin_endpoint() -> String {
    format!(r"\\.\pipe\typedb-admin-bin-smoke-{}", std::process::id())
}

struct ServerGuard(Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn admin_binary_connects_to_server_binary() {
    let server_bin = bin_path("typedb_server_bin");
    let admin_bin = bin_path("typedb_admin_bin");
    assert!(
        server_bin.exists(),
        "{server_bin:?} not built. Run `cargo build --bin typedb_server_bin --bin typedb_admin_bin` first."
    );
    assert!(
        admin_bin.exists(),
        "{admin_bin:?} not built. Run `cargo build --bin typedb_server_bin --bin typedb_admin_bin` first."
    );

    let endpoint = admin_endpoint();
    let data_dir = env::temp_dir().join(format!("typedb-bin-smoke-data-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    #[cfg(unix)]
    let _ = std::fs::remove_file(&endpoint);

    let config = workspace_root().join("server").join("config.yml");
    let server = Command::new(&server_bin)
        .args([
            "--config".to_string(),
            config.to_string_lossy().into_owned(),
            "--development-mode.enabled=true".to_string(),
            "--server.admin.enabled=true".to_string(),
            format!("--server.admin.socket-path={endpoint}"),
            "--server.http.enabled=false".to_string(),
            "--server.listen-address=127.0.0.1:11733".to_string(),
            format!("--storage.data-directory={}", data_dir.display()),
        ])
        .spawn()
        .expect("spawn server");
    let _server_guard = ServerGuard(server);

    thread::sleep(Duration::from_secs(15));

    let admin = Command::new(&admin_bin)
        .args(["--socket-path", &endpoint, "--command", "server version"])
        .output()
        .expect("spawn admin");

    let _ = std::fs::remove_dir_all(&data_dir);
    #[cfg(unix)]
    let _ = std::fs::remove_file(&endpoint);

    assert!(
        admin.status.success(),
        "admin exited {:?}\nstdout:\n{}\nstderr:\n{}",
        admin.status.code(),
        String::from_utf8_lossy(&admin.stdout),
        String::from_utf8_lossy(&admin.stderr),
    );
    let stdout = String::from_utf8_lossy(&admin.stdout);
    assert!(stdout.contains("TypeDB"), "expected 'TypeDB' in admin stdout, got: {stdout}");
}
