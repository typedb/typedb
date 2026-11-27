/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fs,
    io::Write,
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

const DATABASE: &str = "upgrade";
const HTTP_PORT: u16 = 8000;
const SERVER_LOG: &str = "server.log";
const SERVER_MAX_START_TIME: Duration = Duration::from_secs(60);
const SERVER_MAX_STOP_TIME: Duration = Duration::from_secs(60);
const HTTP_MAX_TIME_SECONDS: &str = "120";
const CHECKPOINT_MAX_WAIT: Duration = Duration::from_secs(180); // previous version checkpoints every 60s

fn build_cmd(cmd_str: &str) -> Command {
    let mut cmd = Command::new("sh");
    cmd.arg("-c").arg(cmd_str);
    cmd
}

fn run(cmd_str: &str) -> Output {
    let output = build_cmd(cmd_str).output().unwrap();
    assert!(output.status.success(), "'{cmd_str}' failed: {output:?}");
    output
}

fn extract(archive: &str, into: &str) -> PathBuf {
    fs::create_dir_all(into).unwrap();
    if archive.ends_with(".zip") {
        run(&format!("unzip -q {archive} -d {into}"));
    } else {
        run(&format!("tar -xf {archive} -C {into}"));
    }
    let root = fs::read_dir(into).unwrap().next().unwrap().unwrap().path();
    fs::canonicalize(root).unwrap()
}

fn server_log() -> String {
    fs::read_to_string(SERVER_LOG).unwrap_or_default()
}

fn start_server(distribution: &Path, data: &Path) -> Child {
    let log = fs::OpenOptions::new().create(true).append(true).open(SERVER_LOG).unwrap();
    let mut server = Command::new(distribution.join("typedb"))
        .arg("server")
        .arg("--development-mode.enabled=true")
        .arg(format!("--storage.data-directory={}", data.display()))
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    let started_at = Instant::now();
    let addr = SocketAddr::from(([127, 0, 0, 1], HTTP_PORT));
    while TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_err() {
        if let Some(status) = server.try_wait().unwrap() {
            panic!("server {} exited on start ({status}):\n{}", distribution.display(), server_log());
        }
        assert!(
            started_at.elapsed() < SERVER_MAX_START_TIME,
            "server {} did not start:\n{}",
            distribution.display(),
            server_log()
        );
        thread::sleep(Duration::from_millis(500));
    }
    server
}

fn stop_server(mut server: Child) {
    run(&format!("kill -s TERM {}", server.id()));
    let stopping_at = Instant::now();
    let status = loop {
        if let Some(status) = server.try_wait().unwrap() {
            break status;
        }
        if stopping_at.elapsed() > SERVER_MAX_STOP_TIME {
            server.kill().unwrap();
            panic!("server did not shut down within {SERVER_MAX_STOP_TIME:?}:\n{}", server_log());
        }
        thread::sleep(Duration::from_millis(200));
    };
    assert!(status.success(), "server did not shut down cleanly ({status}):\n{}", server_log());
}

fn http(token: Option<&str>, method: &str, path: &str, body: &str) -> String {
    let mut cmd = Command::new("curl");
    cmd.args([
        "--silent",
        "--show-error",
        "--fail-with-body",
        "--max-time",
        HTTP_MAX_TIME_SECONDS,
        "-X",
        method,
        &format!("http://localhost:{HTTP_PORT}/v1/{path}"),
    ]);
    cmd.args(["-H", "Content-Type: application/json", "--data-binary", "@-"]);
    if let Some(token) = token {
        cmd.args(["-H", &format!("Authorization: Bearer {token}")]);
    }
    let mut child = cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    child.stdin.take().unwrap().write_all(body.as_bytes()).unwrap();
    let output = child.wait_with_output().unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        output.status.success(),
        "{method} {path} failed: {stdout} {}\nserver log:\n{}",
        String::from_utf8_lossy(&output.stderr),
        server_log()
    );
    stdout
}

fn sign_in() -> String {
    let response = http(None, "POST", "signin", r#"{"username":"admin","password":"password"}"#);
    response.split('"').nth(3).expect("no token in signin response").to_owned()
}

fn query(token: &str, transaction_type: &str, query: &str) -> String {
    let query = query.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n");
    let body = format!(
        r#"{{"databaseName":"{DATABASE}","transactionType":"{transaction_type}","query":"{query}","commit":true}}"#
    );
    http(Some(token), "POST", "query", &body)
}

fn latest_checkpoint(data: &Path) -> Option<PathBuf> {
    let mut checkpoints: Vec<PathBuf> = fs::read_dir(data.join(DATABASE).join("checkpoint"))
        .ok()?
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_none())
        .collect();
    checkpoints.sort();
    checkpoints.pop()
}

fn wait_for_checkpoint(data: &Path, newer_than: Option<&Path>) -> PathBuf {
    let started_at = Instant::now();
    loop {
        if let Some(checkpoint) = latest_checkpoint(data).filter(|checkpoint| Some(checkpoint.as_path()) != newer_than)
        {
            return checkpoint;
        }
        assert!(started_at.elapsed() < CHECKPOINT_MAX_WAIT, "no new checkpoint was written");
        thread::sleep(Duration::from_secs(1));
    }
}

#[test]
fn database_from_previous_version_is_upgraded() {
    let current = extract(&std::env::var("TYPEDB_ASSEMBLY_ARCHIVE").unwrap(), "current");
    let previous = extract(&std::env::var("TYPEDB_PREVIOUS_ARCHIVE").unwrap(), "previous");
    let data = std::env::current_dir().unwrap().join("upgrade-data");

    let server = start_server(&previous, &data);
    let token = sign_in();
    http(Some(&token), "POST", &format!("databases/{DATABASE}"), "");
    query(&token, "schema", "define entity person, owns name; attribute name, value string;");
    query(&token, "write", r#"insert $a isa person, has name "Alice"; $b isa person, has name "Bob";"#);
    let previous_checkpoint = wait_for_checkpoint(&data, None);
    stop_server(server);
    assert!(!previous_checkpoint.join("VECTOR_INDEX").exists(), "the previous version must not write a vector index");

    let server = start_server(&current, &data);
    let token = sign_in();
    let answer = query(&token, "read", "match $p isa person, has name $n;");
    assert!(answer.contains("Alice") && answer.contains("Bob"), "data from the previous version is missing: {answer}");
    query(
        &token,
        "schema",
        r#"define attribute embedding, value vector(3, "float32") @index(hnsw:cosine); person owns embedding;"#,
    );
    query(
        &token,
        "write",
        r#"match $a isa person, has name "Alice"; $b isa person, has name "Bob";
           insert $a has embedding vector([1.0, 0.0, 0.0], "float32"); $b has embedding vector([0.0, 1.0, 0.0], "float32");"#,
    );
    let current_checkpoint = wait_for_checkpoint(&data, Some(&previous_checkpoint));
    stop_server(server);
    assert!(current_checkpoint.join("VECTOR_INDEX").exists(), "the checkpoint was not upgraded");

    let server = start_server(&current, &data);
    let answer = query(
        &sign_in(),
        "read",
        r#"match let $e, $_ in cosine_similarity_search(embedding, vector([1.0, 0.0, 0.0], "float32"), -1.0);
           $p isa person, has embedding $e, has name $n;"#,
    );
    stop_server(server);
    assert!(
        answer.contains("Alice") && answer.contains("Bob"),
        "vectors were not restored from the checkpoint: {answer}"
    );
}
