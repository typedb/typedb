/*
 * Copyright (C) 2023 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#![deny(unused_must_use)]
#![deny(rust_2018_idioms)]

use std::process::Command;

use tempdir::TempDir;

#[test]
fn crash() {
    let message = "hello there";

    let streamer = std::env::var("TEST_WAL_STREAMER").unwrap();
    let recoverer = std::env::var("TEST_WAL_RECOVERER").unwrap();

    for _ in 0..10 {
        let directory = TempDir::new("wal-test").unwrap();

        let mut streamer_process = Command::new(&streamer).arg(directory.as_ref()).arg(message).spawn().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        streamer_process.kill().unwrap();

        let output = Command::new(&recoverer).arg(directory.as_ref()).output().unwrap();
        assert!(output.status.success(), "{}\n{}", output.status, String::from_utf8_lossy(&output.stderr));

        let stdout = String::from_utf8(output.stdout).unwrap();
        for (i, line) in stdout.lines().enumerate() {
            assert_eq!(line, format!(r#"{i} "{message} {i}""#));
        }
    }
}
