/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::process::{Child, Command, Output};

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
                .output().expect("Failed to run kill command");
            if !output.status.success() {
                println!("kill-ing process failed: {:?}", output);
            }
        }
    }
    process.wait_with_output()
}

#[test]
fn test_assembly() {
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

    let extract_output = build_cmd(extract_cmd.as_str())
        .output().expect("Failed to run tar");
    if !extract_output.status.success() {
        panic!("{:?}", extract_output);
    }
    let server_process = build_cmd("typedb-extracted/typedb_server_bin").spawn().expect("Failed to spawn server process");
    let test_result = run_test_against_server();
    let server_process_output = kill_process(server_process);

    if test_result.is_err() {
        println!("Server process output:\n{:?}", server_process_output);
        test_result.unwrap();
    }
}

fn run_test_against_server() -> Result<(),()> {
    // TODO
    Ok(())
}
