/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fs,
    io::Read,
    path::Path,
    process::{Child, Command, Output},
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

// Pattern borrowed from tests/behaviour/service/http/http_steps/lib.rs::wait_server_start.
const SERVER_START_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const SERVER_MAX_START_TIME: Duration = Duration::from_secs(30);

fn start_server() -> (Child, u16) {
    start_server_with_env(None, None)
}

/// Poll for either a server crash or for the gRPC port becoming reachable.
/// Mirrors the loop shape of `Context::wait_server_start` in http_steps;
/// uses TCP connect rather than HTTP `/health` to avoid pulling an HTTP client
/// into the assembly test target. Returns true if the process exited.
fn wait_server_start(process: &mut Child, port: u16) -> bool {
    let starting_since = Instant::now();
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    loop {
        if process.try_wait().unwrap().is_some() {
            return true;
        }
        if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
            return false;
        }
        if Instant::now().duration_since(starting_since) > SERVER_MAX_START_TIME {
            panic!("Server has not started in {:?}. Aborting tests!", SERVER_MAX_START_TIME);
        }
        thread::sleep(SERVER_START_CHECK_INTERVAL);
    }
}

fn start_server_with_env(env: Option<(&str, &str)>, expected_fail: Option<&str>) -> (Child, u16) {
    let mut port = 1729;
    loop {
        let mut cmd = build_server_cmd(port);
        if let Some((var, value)) = env {
            cmd.env(var, value);
        }

        let mut process = cmd.spawn().expect("Failed to run server");
        let crashed = wait_server_start(&mut process, port);

        if crashed {
            let mut buf = String::new();
            process.stderr.as_mut().unwrap().read_to_string(&mut buf).unwrap();
            if let Some(fail) = expected_fail {
                if buf.contains(fail) {
                    break (process, port);
                }
            }
            if !buf.contains("SRO11") {
                panic!("Server process crashed for an unrelated reason: {buf}");
            }
        } else {
            break (process, port);
        }
        port += 1;
    }
}

#[test]
fn test_fail_point_always() {
    extract_typedb();
    for fail_point in fail_point::ALL {
        let directive = &format!("{fail_point}=panic");

        delete_data();
        let (mut server_process, port) =
            start_server_with_env(Some((fail_point::FAIL_POINT_ENV, directive)), Some(fail_point));
        setup(port);

        run_test_against_server(&mut server_process, fail_point, port);
        match server_process.try_wait().unwrap() {
            None => {
                kill_process(server_process).unwrap();
                // some fail points are only triggered on second boot
                (server_process, _) =
                    start_server_with_env(Some((fail_point::FAIL_POINT_ENV, directive)), Some(fail_point));
                if server_process.try_wait().unwrap().is_none() {
                    kill_process(server_process).unwrap();
                    panic!("Fail point {fail_point} is never triggered");
                }
                server_process.wait_with_output().unwrap()
            }
            Some(_) => server_process.wait_with_output().unwrap(),
        };

        assert_boots(fail_point);
    }
}

#[test]
fn test_fail_point_chance() {
    extract_typedb();
    for fail_point in fail_point::ALL {
        let directive = &format!("{fail_point}=90%5*print->panic"); // 10% chance to panic, but guaranteed on the 6th

        delete_data();
        let (mut server_process, mut port) =
            start_server_with_env(Some((fail_point::FAIL_POINT_ENV, directive)), Some(fail_point));
        setup(port);

        for _ in 0..10 {
            if server_process.try_wait().unwrap().is_some() {
                break; // crashed
            }
            kill_process(server_process).unwrap();

            (server_process, port) =
                start_server_with_env(Some((fail_point::FAIL_POINT_ENV, directive)), Some(fail_point));
            for _ in 0..6 {
                run_test_against_server(&mut server_process, fail_point, port);
                if server_process.try_wait().unwrap().is_some() {
                    break; // crashed
                }
            }
        }

        if server_process.try_wait().unwrap().is_none() {
            kill_process(server_process).unwrap(); // did not crash; must be a fail point that triggers at most once per run and we got unlucky
        }

        assert_boots(fail_point);
    }
}

fn assert_boots(fail_point: &str) {
    let (mut server_process, _) = start_server();
    match server_process.try_wait().unwrap() {
        None => _ = kill_process(server_process).unwrap(),
        Some(_) => {
            let server_process_output = kill_process(server_process).unwrap();
            eprintln!("Server process output:\n{:?}", server_process_output);
            panic!("Server could not boot after fail point {fail_point} triggered");
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

fn delete_data() {
    fs::remove_dir_all("typedb-extracted/server/data").unwrap();
}

fn setup(port: u16) {
    let command = "
        database create foo
        transaction schema foo
        define entity person, owns name @card(0..1); attribute name, value string; end;
        commit
        ";
    build_console_command(command, port).output().expect("Failed to run console script");
}

fn run_test_against_server(server_process: &mut Child, fail_point_name: &str, port: u16) {
    enum Instruction {
        Command(&'static str),
        Parallel(&'static str),
        WaitForCheckpoint,
    }

    let instructions = [
        // 1. Write the schema. Checks for crashes during commit.
        Instruction::Command(
            "
            transaction schema foo
            define entity person, owns name @card(0..1); attribute name, value string; end;
            commit
            ",
        ),
        // 2. Wait for checkpoint. Check for crashes during various stages of a checkpoint.
        Instruction::WaitForCheckpoint,
        // 3. Write a data commit. This is mostly just to trigger another checkpoint.
        Instruction::Command(
            r#"
            transaction write foo
            insert $john isa person, has name "John"; end;
            commit
            "#,
        ),
        // 4. Wait for checkpoint. Check for crashes during cleanup of old checkpoints.
        Instruction::WaitForCheckpoint,
        // 5. Run a bunch of parallel transactions with conflicts. Check for crashes during aborted commits specifically.
        Instruction::Parallel(
            r#"
            transaction write foo
            match $john isa person, has name $name;
            delete has $name of $john;
            insert $john has name "<value>";
            end;
            commit
            "#,
        ),
        // 6. Delete the database. Check for crashes during cleanup, and resiliency to partially deleted DB.
        Instruction::Command("database delete foo"),
        // 7. Recreate the database. Mainly to be able to rerun this function multiple times.
        Instruction::Command("database create foo"),
        Instruction::Command(
            "
            transaction schema foo
            define entity person, owns name @card(0..1); attribute name, value string; end;
            commit
            ",
        ),
    ];

    for inst in instructions {
        match inst {
            Instruction::Command(command) => {
                let status =
                    build_console_command(command, port).output().expect("Failed to run console script").status;
                if !status.success() {
                    break;
                }
            }
            Instruction::Parallel(command) => {
                let cmds = std::array::from_fn::<_, 10, _>(|i| {
                    build_console_command(&command.replace("<value>", &format!("{i}")), port)
                });
                let threads =
                    cmds.into_iter().map(|mut cmd| thread::spawn(move || cmd.output().unwrap())).collect::<Vec<_>>();
                for thread in threads {
                    thread.join().unwrap();
                }
            }
            Instruction::WaitForCheckpoint => {
                if fail_point_name.contains("CHECKPOINT") {
                    wait_process_timeout(server_process, CHECKPOINT_INTERVAL).unwrap()
                }
            }
        }
    }
}

fn build_server_cmd(port: u16) -> Command {
    let mut cmd = Command::new("typedb-extracted/typedb");
    cmd.arg("server")
        .arg(format!("--server.address=0.0.0.0:{port}"))
        .arg("--development-mode.enabled=true")
        .arg("--diagnostics.monitoring.enabled=false")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    cmd
}

fn build_console_command(command: &str, port: u16) -> Command {
    let mut cmd = Command::new("typedb-extracted/typedb");
    cmd.arg("console")
        .arg("--username=admin")
        .arg("--password=password")
        .arg(format!("--address=localhost:{port}"))
        .arg("--tls-disabled")
        .arg("--command")
        .arg(command);
    cmd
}
