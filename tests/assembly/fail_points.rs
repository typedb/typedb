/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fs,
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

const BOOT_DURATION: Duration = Duration::from_secs(10);

macro_rules! start_server {
    () => {{
        let process = build_cmd("typedb-extracted/typedb server --development-mode.enabled=true")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("Failed to run console script");
        std::thread::sleep(BOOT_DURATION);
        process
    }};
    ($env:expr => $value:expr) => {{
        let process = build_cmd("typedb-extracted/typedb server --development-mode.enabled=true")
            .env($env, $value)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("Failed to run console script");
        std::thread::sleep(BOOT_DURATION);
        process
    }};
}

#[test]
fn test_fail_point_always() {
    for fail_point in fail_point::ALL {
        let directive = format!("{fail_point}=panic");

        extract_typedb();
        let mut server_process = start_server!(fail_point::FAIL_POINT_ENV => &directive);
        setup();

        run_test_against_server(&mut server_process);
        if server_process.try_wait().unwrap().is_none() {
            kill_process(server_process).unwrap();
            // some fail points are only triggered on second boot
            let mut server_process = start_server!(fail_point::FAIL_POINT_ENV => directive);
            if server_process.try_wait().unwrap().is_none() {
                kill_process(server_process).unwrap();
                panic!("Fail point {fail_point} is never triggered");
            }
        }

        assert_boots(fail_point);
    }
}

#[ignore]
#[test]
fn test_fail_point_chance() {
    for fail_point in fail_point::ALL {
        let directive = format!("{fail_point}=90%5*print->panic"); // 10% chance to panic, but guaranteed on the 6th

        extract_typedb();
        let mut server_process = start_server!(fail_point::FAIL_POINT_ENV => &directive);
        setup();

        for _ in 0..10 {
            if server_process.try_wait().unwrap().is_some() {
                break; // crashed
            }

            kill_process(server_process).unwrap();
            server_process = start_server!(fail_point::FAIL_POINT_ENV => &directive);

            for _ in 0..6 {
                run_test_against_server(&mut server_process);
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
    let mut server_process = start_server!();
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

fn setup() {
    let command = "
        database create foo
        transaction schema foo
        define entity person, owns name @card(0..1); attribute name, value string; end;
        commit
        ";
    build_console_command(command).output().expect("Failed to run console script");
}

fn run_test_against_server(server_process: &mut Child) {
    enum Instruction {
        Command(&'static str),
        Parallel(&'static str),
        Sleep(Duration),
    }

    let instructions = [
        Instruction::Command(
            "
            transaction schema foo
            define entity person, owns name @card(0..1); attribute name, value string; end;
            commit
            ",
        ),
        Instruction::Sleep(CHECKPOINT_INTERVAL),
        Instruction::Command(
            r#"
            transaction write foo
            insert $john isa person, has name "John"; end;
            commit
            "#,
        ),
        Instruction::Sleep(CHECKPOINT_INTERVAL),
        Instruction::Parallel(
            "
            transaction write foo
            match $john isa person, has name $name;
            delete has $name of $john;
            insert $john has $name;
            end;
            commit
            ",
        ),
        Instruction::Command("database delete foo"),
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
                let status = build_console_command(command).output().expect("Failed to run console script").status;
                if !status.success() {
                    break;
                }
            }
            Instruction::Parallel(command) => {
                let cmds = std::array::from_fn::<_, 10, _>(|_| build_console_command(command));
                let threads =
                    cmds.into_iter().map(|mut cmd| thread::spawn(move || cmd.output().unwrap())).collect::<Vec<_>>();
                for thread in threads {
                    thread.join().unwrap();
                }
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
