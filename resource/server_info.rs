/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#[derive(Clone, Debug)]
pub struct ServerInfo {
    pub logo: &'static str,
    pub distribution: &'static str,
    pub version: &'static str,
}

impl ServerInfo {
    pub const fn new(logo: &'static str, distribution: &'static str, version: &'static str) -> Self {
        ServerInfo { logo, distribution, version }
    }
}
