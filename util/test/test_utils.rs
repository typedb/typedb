/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cell::OnceCell,
    fs,
    ops::Deref,
    path::{Path, PathBuf},
    sync::Mutex,
};

use logger::initialise_logging;
use tracing::subscriber::DefaultGuard;

pub static LOGGING_GUARD: Mutex<OnceCell<DefaultGuard>> = Mutex::new(OnceCell::new());

pub fn init_logging() {
    LOGGING_GUARD.lock().unwrap().get_or_init(initialise_logging);
}

#[derive(Debug)]
pub struct TempDir(PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).ok();
    }
}

impl AsRef<Path> for TempDir {
    fn as_ref(&self) -> &Path {
        self
    }
}

impl Deref for TempDir {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub fn create_tmp_dir() -> TempDir {
    let id = rand::random::<u64>();
    let dir_name = format!("test_storage_{}", id);
    let dir = std::env::temp_dir().join(Path::new(&dir_name));
    fs::create_dir_all(&dir).unwrap();
    TempDir(dir)
}
