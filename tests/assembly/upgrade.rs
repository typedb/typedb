/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// A database created by the previous release must open in the current one, which upgrades
// its storage by adding the vector index to the checkpoint.

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
const VECTOR_SEARCH: &str = "--experimental-vector-search-enabled=true";
const SERVER_MAX_START_TIME: Duration = Duration::from_secs(60);
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
    // each archive holds a single top-level `typedb-all-<platform>-<version>` directory
    let root = fs::read_dir(into).unwrap().next().unwrap().unwrap().path();
    fs::canonicalize(root).unwrap()
}

fn start_server(distribution: &Path, data: &Path, extra_args: &str) -> Child {
    let mut server = build_cmd(&format!(
        "{}/typedb server --development-mode.enabled=true --storage.data-directory={} {extra_args}",
        distribution.display(),
        data.display()
    ))
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .unwrap();
    let started_at = Instant::now();
    let addr = SocketAddr::from(([127, 0, 0, 1], HTTP_PORT));
    while TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_err() {
        if server.try_wait().unwrap().is_some() {
            panic!("server {} exited on start: {:?}", distribution.display(), server.wait_with_output());
        }
        assert!(started_at.elapsed() < SERVER_MAX_START_TIME, "server {} did not start", distribution.display());
        thread::sleep(Duration::from_millis(500));
    }
    server
}

// SIGTERM, not SIGKILL: the server must shut down gracefully so it writes its final checkpoint
fn stop_server(server: Child) {
    run(&format!("kill -s TERM {}", server.id()));
    let output = server.wait_with_output().unwrap();
    assert!(output.status.success(), "server did not shut down cleanly: {output:?}");
}

// The console bundled with the assembly may predate vector syntax, so queries go over HTTP
fn http(token: Option<&str>, method: &str, path: &str, body: &str) -> String {
    let mut cmd = Command::new("curl");
    cmd.args([
        "--silent",
        "--show-error",
        "--fail-with-body",
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
    assert!(output.status.success(), "{method} {path} failed: {stdout} {}", String::from_utf8_lossy(&output.stderr));
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

// completed checkpoints only: one still being written has a `.tmp` extension
fn latest_checkpoint(data: &Path) -> Option<PathBuf> {
    let mut checkpoints: Vec<PathBuf> = fs::read_dir(data.join(DATABASE).join("checkpoint"))
        .ok()?
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_none())
        .collect();
    checkpoints.sort();
    checkpoints.pop()
}

// Servers checkpoint periodically, not on shutdown, so the test waits for one to be written
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

    let server = start_server(&previous, &data, "");
    let token = sign_in();
    http(Some(&token), "POST", &format!("databases/{DATABASE}"), "");
    query(&token, "schema", "define entity person, owns name; attribute name, value string;");
    query(&token, "write", r#"insert $a isa person, has name "Alice"; $b isa person, has name "Bob";"#);
    // so the current version opens a checkpoint lacking the vector index, not only a WAL
    let previous_checkpoint = wait_for_checkpoint(&data, None);
    stop_server(server);
    assert!(!previous_checkpoint.join("VECTOR_INDEX").exists(), "the previous version must not write a vector index");

    let server = start_server(&current, &data, VECTOR_SEARCH);
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

    let server = start_server(&current, &data, VECTOR_SEARCH);
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
