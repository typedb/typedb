/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub fn pack_result<T, OK, ERR>(result: Result<OK, ERR>, t: T) -> Result<(OK, T), (ERR, T)> {
    match result {
        Ok(ok) => Ok((ok, t)),
        Err(err) => Err((err, t)),
    }
}

pub fn unpack_result<T, OK, ERR>(result: Result<(OK, T), (ERR, T)>) -> (Result<OK, ERR>, T) {
    match result {
        Ok((ok, t)) => (Ok(ok), t),
        Err((err, t)) => (Err(err), t),
    }
}
