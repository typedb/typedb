/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// End-to-end test for the assembled admin tool. Runs in two modes:
//
// * Archive mode (`TYPEDB_ASSEMBLY_ARCHIVE` env var set, used by bazel rust_test
//   //admin:test_admin_assembly on Linux/macOS): extracts the distribution
//   archive and invokes the binaries through the bash launcher.
//
// * Direct mode (env var unset, used by cargo on Windows where bazel is not currently supported):
//   uses cargo-built binaries at `target/debug/` directly.

use std::{
    env,
    io::Write,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::OnceLock,
    thread,
    time::Duration,
};

const EXTRACTED_DIR: &str = "typedb-extracted";

fn archive_mode() -> Option<String> {
    env::var("TYPEDB_ASSEMBLY_ARCHIVE").ok()
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).parent().unwrap().to_path_buf()
}

static EXTRACTED_ONCE: OnceLock<()> = OnceLock::new();

fn extract_archive_once(archive_name: &str) {
    EXTRACTED_ONCE.get_or_init(|| {
        let extract_cmd = if archive_name.ends_with(".zip") {
            let extracted_dir = archive_name.replace(".zip", "-0.0.0");
            format!("unzip -o {archive_name} && mv {extracted_dir} {EXTRACTED_DIR}")
        } else if archive_name.ends_with(".tar.gz") {
            let extracted_dir = archive_name.replace(".tar.gz", "-0.0.0");
            format!("tar -xf {archive_name} && mv {extracted_dir} {EXTRACTED_DIR}")
        } else {
            panic!("Expected .zip or .tar.gz, got {archive_name}");
        };
        let out = Command::new("sh").arg("-c").arg(&extract_cmd).output().expect("extract archive");
        assert!(out.status.success(), "extract failed: {out:?}");
    });
}

fn typedb_command(subcommand: &str) -> Command {
    match archive_mode() {
        Some(_) => {
            let mut cmd = Command::new(format!("{EXTRACTED_DIR}/typedb"));
            cmd.arg(subcommand);
            cmd
        }
        None => {
            let suffix = if cfg!(windows) { ".exe" } else { "" };
            let bin = workspace_root().join("target").join("debug").join(format!("typedb_{subcommand}_bin{suffix}"));
            assert!(
                bin.exists(),
                "{bin:?} not built. Run `cargo build -p typedb_server_bin -p typedb_admin_bin` first."
            );
            Command::new(bin)
        }
    }
}

#[cfg(unix)]
fn admin_endpoint(tag: &str) -> String {
    if archive_mode().is_some() {
        format!("{EXTRACTED_DIR}/admin-{tag}.sock")
    } else {
        env::temp_dir()
            .join(format!("typedb-admin-bin-{tag}-{}.sock", std::process::id()))
            .to_string_lossy()
            .into_owned()
    }
}

#[cfg(windows)]
fn admin_endpoint(tag: &str) -> String {
    format!(r"\\.\pipe\typedb_admin_{tag}_{}", std::process::id())
}

fn data_dir(tag: &str) -> PathBuf {
    if archive_mode().is_some() {
        PathBuf::from(format!("typedb-data-{tag}"))
    } else {
        env::temp_dir().join(format!("typedb-bin-{tag}-data-{}", std::process::id()))
    }
}

fn setup(tag: &str) -> (String, PathBuf) {
    if let Some(archive) = archive_mode() {
        extract_archive_once(&archive);
    }
    let endpoint = admin_endpoint(tag);
    let data_dir = data_dir(tag);
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    #[cfg(unix)]
    let _ = std::fs::remove_file(&endpoint);
    (endpoint, data_dir)
}

fn cleanup(endpoint: &str, data_dir: &PathBuf) {
    let _ = std::fs::remove_dir_all(data_dir);
    #[cfg(unix)]
    let _ = std::fs::remove_file(endpoint);
    #[cfg(windows)]
    let _ = endpoint;
}

struct ServerGuard(Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_server(endpoint: &str, data_dir: &PathBuf, listen_port: u16) -> Child {
    let mut cmd = typedb_command("server");
    // The launcher (archive mode) sets TYPEDB_HOME so the server finds config.yml on
    // its own. The direct binary needs an explicit --config.
    if archive_mode().is_none() {
        let config = workspace_root().join("server").join("config.yml");
        cmd.arg("--config").arg(config);
    }
    cmd.args([
        "--development-mode.enabled=true".to_string(),
        "--server.admin.enabled=true".to_string(),
        format!("--server.admin.socket-path={endpoint}"),
        "--server.http.enabled=false".to_string(),
        format!("--server.listen-address=127.0.0.1:{listen_port}"),
        format!("--storage.data-directory={}", data_dir.display()),
    ]);
    cmd.spawn().expect("spawn server")
}

#[test]
fn test_admin_assembly() {
    let (endpoint, data_dir) = setup("smoke");
    let _server_guard = ServerGuard(spawn_server(&endpoint, &data_dir, 11733));
    thread::sleep(Duration::from_secs(15));

    let admin = typedb_command("admin")
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
