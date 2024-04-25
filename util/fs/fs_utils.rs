/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fs, io, path::Path};

pub fn copy_dir_all(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    fs::create_dir(&to)?;
    for entry in fs::read_dir(&from)? {
        let entry = entry?;
        let from = entry.path();
        let to = to.as_ref().join(from.file_name().unwrap());
        if entry.metadata()?.is_dir() {
            copy_dir_all(from, to)?;
        } else {
            fs::copy(from, to)?;
        }
    }
    Ok(())
}
