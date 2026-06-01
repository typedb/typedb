/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    env,
    io::Write,
    path::PathBuf,
    process::{Child, Command, Stdio},
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
fn admin_endpoint(tag: &str) -> String {
    env::temp_dir().join(format!("typedb-admin-bin-{tag}-{}.sock", std::process::id())).to_string_lossy().into_owned()
}

#[cfg(windows)]
fn admin_endpoint(tag: &str) -> String {
    format!(r"\\.\pipe\typedb-admin-bin-{tag}-{}", std::process::id())
}

fn data_dir(tag: &str) -> PathBuf {
    env::temp_dir().join(format!("typedb-bin-{tag}-data-{}", std::process::id()))
}

fn assert_bins_present(server_bin: &PathBuf, admin_bin: &PathBuf) {
    assert!(
        server_bin.exists(),
        "{server_bin:?} not built. Run `cargo build -p typedb_server_bin -p typedb_admin_bin` first."
    );
    assert!(
        admin_bin.exists(),
        "{admin_bin:?} not built. Run `cargo build -p typedb_server_bin -p typedb_admin_bin` first."
    );
}

struct ServerGuard(Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_server(server_bin: &PathBuf, endpoint: &str, data_dir: &PathBuf, listen_port: u16) -> Child {
    let config = workspace_root().join("server").join("config.yml");
    Command::new(server_bin)
        .args([
            "--config".to_string(),
            config.to_string_lossy().into_owned(),
            "--development-mode.enabled=true".to_string(),
            "--server.admin.enabled=true".to_string(),
            format!("--server.admin.socket-path={endpoint}"),
            "--server.http.enabled=false".to_string(),
            format!("--server.listen-address=127.0.0.1:{listen_port}"),
            format!("--storage.data-directory={}", data_dir.display()),
        ])
        .spawn()
        .expect("spawn server")
}

fn cleanup(endpoint: &str, data_dir: &PathBuf) {
    let _ = std::fs::remove_dir_all(data_dir);
    #[cfg(unix)]
    let _ = std::fs::remove_file(endpoint);
    #[cfg(windows)]
    let _ = endpoint;
}

#[test]
fn test_admin_assembly() {
    let server_bin = bin_path("typedb_server_bin");
    let admin_bin = bin_path("typedb_admin_bin");
    assert_bins_present(&server_bin, &admin_bin);

    let endpoint = admin_endpoint("smoke");
    let data_dir = data_dir("smoke");
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    #[cfg(unix)]
    let _ = std::fs::remove_file(&endpoint);

    let _server_guard = ServerGuard(spawn_server(&server_bin, &endpoint, &data_dir, 11733));
    thread::sleep(Duration::from_secs(15));

    let admin = Command::new(&admin_bin)
        .args(["--socket-path", &endpoint, "--command", "server version"])
        .output()
        .expect("spawn admin");

    cleanup(&endpoint, &data_dir);

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

#[test]
fn test_admin_assembly_password_from_stdin() {
    let server_bin = bin_path("typedb_server_bin");
    let admin_bin = bin_path("typedb_admin_bin");
    assert_bins_present(&server_bin, &admin_bin);

    let endpoint = admin_endpoint("stdin");
    let data_dir = data_dir("stdin");
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    #[cfg(unix)]
    let _ = std::fs::remove_file(&endpoint);

    let _server_guard = ServerGuard(spawn_server(&server_bin, &endpoint, &data_dir, 11734));
    thread::sleep(Duration::from_secs(15));

    let mut admin = Command::new(&admin_bin)
        .args(["--socket-path", &endpoint, "--command", "user reset-password admin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn admin");
    admin
        .stdin
        .as_mut()
        .expect("admin stdin pipe")
        .write_all(b"piped pw with spaces!@#\n")
        .expect("write password to admin stdin");
    let admin = admin.wait_with_output().expect("admin output");

    cleanup(&endpoint, &data_dir);

    assert!(
        admin.status.success(),
        "admin exited {:?}\nstdout:\n{}\nstderr:\n{}",
        admin.status.code(),
        String::from_utf8_lossy(&admin.stdout),
        String::from_utf8_lossy(&admin.stderr),
    );
    let stdout = String::from_utf8_lossy(&admin.stdout);
    assert!(stdout.contains("Password updated"), "expected 'Password updated' in admin stdout, got: {stdout}");
}
