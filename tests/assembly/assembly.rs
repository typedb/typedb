/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fs,
    path::Path,
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

use resource::constants::database::CHECKPOINT_INTERVAL;

fn build_cmd(cmd_str: &str) -> Command {
    let mut cmd = Command::new("sh");
    cmd.arg("-c").arg(cmd_str);
    cmd
}

fn kill_process(mut process: Child) -> std::io::Result<Output> {
    process.kill()?;
    process.wait_with_output()
}

fn wait_process_timeout(process: &mut Child, timeout: Duration) -> std::io::Result<()> {
    let end = Instant::now() + timeout;
    while Instant::now() < end {
        if process.try_wait()?.is_some() {
            return Ok(());
        }
        thread::sleep(Duration::from_secs(1));
    }
    Ok(())
}

#[test]
fn test_assembly() {
    extract_typedb();
    let server_process = build_cmd("typedb-extracted/typedb server --development-mode.enabled=true")
        .spawn()
        .expect("Failed to spawn server process");
    thread::sleep(Duration::from_secs(10));
    let console_process_output = build_cmd(concat!(
        "typedb-extracted/typedb console --username=admin --password=password --address=localhost:1729 ",
        "--tls-disabled --script=tests/assembly/script.tql",
    ))
    .output()
    .expect("Failed to run console script");
    let server_process_output = kill_process(server_process);

    if !console_process_output.status.success() {
        eprintln!("Test failed:");
        eprintln!("Console process output:\n{:?}", console_process_output);
        eprintln!("Server process output:\n{:?}", server_process_output);
        panic!();
    }
}

#[test]
fn test_fail_point_always() {
    for fail_point in fail_point::ALL {
        extract_typedb();
        let mut server_process = build_cmd("typedb-extracted/typedb server --development-mode.enabled=true")
            .env(fail_point::FAIL_POINT_ENV, format!("{fail_point}=panic"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("Failed to spawn server process");
        thread::sleep(Duration::from_secs(10));
        run_test_against_server(&mut server_process);
        wait_process_timeout(&mut server_process, CHECKPOINT_INTERVAL).unwrap();
        if server_process.try_wait().unwrap().is_none() {
            kill_process(server_process).unwrap();
            eprintln!("Fail point {fail_point} is never triggered");
        }

        let mut server_process = build_cmd("typedb-extracted/typedb server --development-mode.enabled=true")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to spawn server process");
        thread::sleep(Duration::from_secs(10));
        match server_process.try_wait().unwrap() {
            None => _ = kill_process(server_process).unwrap(),
            Some(_) => {
                let server_process_output = kill_process(server_process).unwrap();
                eprintln!("Server process output:\n{:?}", server_process_output);
                panic!("Server could not boot after fail point {fail_point} triggered");
            }
        }
    }
}

fn extract_typedb() {
    if fs::exists("typedb-extracted").unwrap() {
        fs::remove_dir_all("typedb-extracted").unwrap();
    }
    let archive_name = std::env::var("TYPEDB_ASSEMBLY_ARCHIVE").unwrap();
    let extract_cmd = if archive_name.ends_with(".zip") {
        let without_extension = Path::new(archive_name.trim_end_matches(".zip")).file_name().unwrap().to_str().unwrap();
        format!("unzip {archive_name} && mv {without_extension} typedb-extracted")
    } else if archive_name.ends_with(".tar.gz") {
        let without_extension = archive_name.replace(".tar.gz", "");
        format!("tar -xf {archive_name} && mv {without_extension} typedb-extracted")
    } else {
        unreachable!("Expected .zip or .tar.gz");
    };

    let extract_output = build_cmd(extract_cmd.as_str()).output().expect("Failed to run tar");
    if !extract_output.status.success() {
        panic!("{:?}", extract_output);
    }
}

fn run_test_against_server(server_process: &mut Child) {
    enum Instruction {
        Command(&'static str),
        Parallel(&'static str),
        Sleep(Duration),
    }

    let instructions = [
        Instruction::Command("database create foo"),
        Instruction::Command(
            r#"
transaction schema foo
define entity person, owns name @card(0..1); attribute name, value string; end;
commit"#,
        ),
        Instruction::Sleep(CHECKPOINT_INTERVAL),
        Instruction::Command(
            r#"
transaction write foo
insert $john isa person, has name "John"; end;
commit"#,
        ),
        Instruction::Sleep(CHECKPOINT_INTERVAL),
        Instruction::Parallel(
            r#"
transaction write foo
match $john isa person, has name $name;
delete has $name of $john; end;
commit"#,
        ),
        Instruction::Command("database delete foo"),
        Instruction::Command("database create foo"),
        Instruction::Command(
            r#"
transaction schema foo
define entity person, owns name @card(0..1); attribute name, value string; end;
commit"#,
        ),
    ];

    for inst in instructions {
        match inst {
            Instruction::Command(command) => {
                let status = build_console_command(command).output().expect("Failed to run console script").status;
                if !status.success() {
                    break;
                }
            }
            Instruction::Parallel(command) => {
                let mut cmd = build_console_command(command);
                let mut cmd2 = build_console_command(command);
                thread::spawn(move || cmd.output().unwrap());
                cmd2.output().unwrap();
            }
            Instruction::Sleep(duration) => wait_process_timeout(server_process, duration).unwrap(),
        }
    }
}

fn build_console_command(command: &str) -> Command {
    build_cmd(&format!(
        concat!(
            "typedb-extracted/typedb console --username=admin --password=password ",
            "--address=localhost:1729 --tls-disabled --command='{command}'"
        ),
        command = command,
    ))
}
