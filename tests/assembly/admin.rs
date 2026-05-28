/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    process::{Child, Command, Output},
    thread,
    time::Duration,
};

fn build_cmd(cmd_str: &str) -> Command {
    let mut cmd = Command::new("sh");
    cmd.arg("-c").arg(cmd_str);
    cmd
}

fn kill_process(process: Child) -> std::io::Result<Output> {
    let mut process = process;
    match process.try_wait() {
        Ok(Some(_)) => {}
        _ => {
            let output = build_cmd(format!("kill -s TERM {}", process.id()).as_str())
                .output()
                .expect("Failed to run kill command");
            if !output.status.success() {
                println!("kill-ing process failed: {:?}", output);
            }
        }
    }
    process.wait_with_output()
}

#[cfg(unix)]
fn admin_endpoint() -> String {
    format!("typedb-extracted/admin-assembly-{}.sock", std::process::id())
}

#[cfg(windows)]
fn admin_endpoint() -> String {
    format!(r"\\.\pipe\typedb-admin-assembly-{}", std::process::id())
}

#[test]
fn test_admin_assembly() {
    let archive_name = std::env::var("TYPEDB_ASSEMBLY_ARCHIVE").unwrap();
    let extract_cmd = if archive_name.ends_with(".zip") {
        let without_extension = archive_name.replace(".zip", "");
        format!("unzip {archive_name} && mv {without_extension} typedb-extracted")
    } else if archive_name.ends_with(".tar.gz") {
        let without_extension = archive_name.replace(".tar.gz", "");
        format!("tar -xf {archive_name} && mv {without_extension} typedb-extracted")
    } else {
        unreachable!("Expected .zip or .tar.gz");
    };

    let extract_output = build_cmd(extract_cmd.as_str()).output().expect("Failed to extract archive");
    if !extract_output.status.success() {
        panic!("Extraction failed: {:?}", extract_output);
    }

    let endpoint = admin_endpoint();
    let server_cmd = format!(
        "typedb-extracted/typedb server \
            --development-mode.enabled=true \
            --server.admin.enabled=true \
            --server.admin.socket-path='{endpoint}'"
    );
    let server_process = build_cmd(&server_cmd).spawn().expect("Failed to spawn server process");
    thread::sleep(Duration::from_secs(10));

    let admin_output = run_admin_command(&endpoint, "server version");
    let server_output = kill_process(server_process);

    if !admin_output.status.success() {
        println!("Admin command failed:");
        println!("Admin process output:\n{:?}", admin_output);
        println!("Server process output:\n{:?}", server_output);
        panic!();
    }
    let stdout = String::from_utf8_lossy(&admin_output.stdout);
    assert!(stdout.contains("TypeDB"), "Expected admin `server version` output to contain 'TypeDB', got: {stdout:?}");
}

fn run_admin_command(endpoint: &str, command: &str) -> Output {
    let cmd = format!("typedb-extracted/typedb admin --socket-path '{endpoint}' --command '{command}'",);
    build_cmd(&cmd).output().expect("Failed to run admin command")
}
