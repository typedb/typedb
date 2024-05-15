/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(rust_2018_idioms)]

use std::process::Command;

use tempdir::TempDir;

fn main() {
    let message = "hello there";

    let streamer = std::env::var("TEST_WAL_STREAMER").unwrap();
    let recoverer = std::env::var("TEST_WAL_RECOVERER").unwrap();

    for i in 0..5 {
        let directory = TempDir::new("wal-test").unwrap();

        let mut streamer_process = Command::new(&streamer).arg(directory.as_ref()).arg(message).spawn().unwrap();
        // WARNING: long sleep otherwise we get nondeterministic failures (on mac). It seems sometimes the subprocess execution is delayed up to 200ms.
        std::thread::sleep(std::time::Duration::from_millis(500));
        streamer_process.kill().unwrap();

        let output = Command::new(&recoverer).arg(directory.as_ref()).output().unwrap();
        assert!(output.status.success(), "{}\n{}", output.status, String::from_utf8_lossy(&output.stderr));

        let stdout = String::from_utf8(output.stdout).unwrap();
        for (i, line) in (1..).zip(stdout.lines()) {
            assert_eq!(line, format!(r#"{i} "{message} {i}""#));
        }
    }
}
