/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#[cfg(unix)]
pub mod unix;
#[cfg(windows)]
pub mod windows;

#[cfg(unix)]
pub use unix::{AdminConnection, AdminListener, AdminPath, bind_admin_endpoint, cleanup_admin_endpoint};
#[cfg(windows)]
pub use windows::{AdminConnection, AdminListener, AdminPath, bind_admin_endpoint, cleanup_admin_endpoint};

pub fn endpoint_to_string(endpoint: &AdminPath) -> String {
    #[cfg(unix)]
    {
        endpoint.to_string_lossy().into_owned()
    }
    #[cfg(windows)]
    {
        endpoint.clone()
    }
}
