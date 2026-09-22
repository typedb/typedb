/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Output to stdout and stderr that never panics: unlike `println!` and `eprintln!`, a failed write is dropped.

use std::io::{self, Write};

/// Writes a line to stdout like `println!`, but never panics when stdout is unwritable.
#[macro_export]
macro_rules! safe_println {
    ($($arg:tt)*) => {{
        use ::std::io::Write as _;
        let _ = ::std::writeln!(::std::io::stdout().lock(), $($arg)*);
    }};
}

/// Writes a line to stderr like `eprintln!`, but never panics when stderr is unwritable.
#[macro_export]
macro_rules! safe_eprintln {
    ($($arg:tt)*) => {{
        use ::std::io::Write as _;
        let _ = ::std::writeln!(::std::io::stderr().lock(), $($arg)*);
    }};
}

/// Stderr as a writer whose failed writes are dropped, like `safe_eprintln!`.
pub(crate) fn safe_stderr() -> SafeWriter<io::Stderr> {
    SafeWriter(io::stderr())
}

/// Wraps a writer so that a failed write or flush is dropped and reported as success.
pub(crate) struct SafeWriter<W>(W);

impl<W: Write> Write for SafeWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let _ = self.0.write_all(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        let _ = self.0.flush();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Write};

    use super::SafeWriter;

    /// A writer whose reader has gone away: every operation fails with `BrokenPipe`.
    struct BrokenPipe;

    impl Write for BrokenPipe {
        fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            Err(io::ErrorKind::BrokenPipe.into())
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::ErrorKind::BrokenPipe.into())
        }
    }

    #[test]
    fn safe_writer_drops_failed_writes() {
        let mut writer = SafeWriter(BrokenPipe);
        assert_eq!(writer.write(b"line\n").unwrap(), 5);
        writer.write_all(b"line\n").unwrap();
        writeln!(writer, "line {}", 1).unwrap();
        writer.flush().unwrap();
    }

    #[test]
    fn safe_writer_passes_successful_writes_through() {
        let mut buffer = Vec::new();
        let mut writer = SafeWriter(&mut buffer);
        assert_eq!(writer.write(b"one\n").unwrap(), 4);
        writer.write_all(b"two\n").unwrap();
        writer.flush().unwrap();
        assert_eq!(buffer, b"one\ntwo\n");
    }
}
