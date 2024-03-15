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

use std::fmt;

/// Taken from 'tracing_unwrap'
pub trait ResultExt<T, E> {
    /// Unwraps a result, yielding the content of an [`Ok`].
    ///
    /// # Panics
    ///
    /// Panics if the value is an [`Err`], logging a message provided by the
    /// [`Err`]'s value to a [`tracing::Subscriber`] at an [`ERROR`] level.
    ///
    /// [`ERROR`]: /tracing/0.1/tracing/struct.Level.html#associatedconstant.ERROR
    fn unwrap_or_log(self) -> T
    where
        E: fmt::Debug;

    /// Unwraps a result, yielding the content of an [`Ok`].
    ///
    /// # Panics
    ///
    /// Panics if the value is an [`Err`], logging the passed message and the
    /// content of the [`Err`] to a [`tracing::Subscriber`] at an [`ERROR`] level.
    ///
    /// [`ERROR`]: /tracing/0.1/tracing/struct.Level.html#associatedconstant.ERROR
    fn expect_or_log(self, msg: &str) -> T
    where
        E: fmt::Debug;

    /// Unwraps a result, yielding the content of an [`Err`].
    ///
    /// # Panics
    ///
    /// Panics if the value is an [`Ok`], logging a message provided by the
    /// [`Ok`]'s value to a [`tracing::Subscriber`] at an [`ERROR`] level.
    ///
    /// [`ERROR`]: /tracing/0.1/tracing/struct.Level.html#associatedconstant.ERROR
    fn unwrap_err_or_log(self) -> E
    where
        T: fmt::Debug;

    /// Unwraps a result, yielding the content of an [`Err`].
    ///
    /// # Panics
    ///
    /// Panics if the value is an [`Ok`], logging the passed message and the
    /// content of the [`Ok`] to a [`tracing::Subscriber`] at an [`ERROR`] level.
    ///
    /// [`ERROR`]: /tracing/0.1/tracing/struct.Level.html#associatedconstant.ERROR
    fn expect_err_or_log(self, msg: &str) -> E
    where
        T: fmt::Debug;
}

impl<T, E> ResultExt<T, E> for Result<T, E> {
    #[inline]
    #[track_caller]
    fn unwrap_or_log(self) -> T
    where
        E: fmt::Debug,
    {
        match self {
            Ok(t) => t,
            Err(e) => failed_with("called `Result::unwrap_or_log()` on an `Err` value", &e),
        }
    }

    #[inline]
    #[track_caller]
    fn expect_or_log(self, msg: &str) -> T
    where
        E: fmt::Debug,
    {
        match self {
            Ok(t) => t,
            Err(e) => failed_with(msg, &e),
        }
    }

    #[inline]
    #[track_caller]
    fn unwrap_err_or_log(self) -> E
    where
        T: fmt::Debug,
    {
        match self {
            Ok(t) => failed_with("called `Result::unwrap_err_or_log()` on an `Ok` value", &t),
            Err(e) => e,
        }
    }

    #[inline]
    #[track_caller]
    fn expect_err_or_log(self, msg: &str) -> E
    where
        T: fmt::Debug,
    {
        match self {
            Ok(t) => failed_with(msg, &t),
            Err(e) => e,
        }
    }
}

//
// Helper functions.
//

#[inline(never)]
#[cold]
#[track_caller]
fn failed_with(msg: &str, value: &dyn fmt::Debug) -> ! {
    #[cfg(feature = "log-location")]
    {
        let location = std::panic::Location::caller();
        tracing::error!(
            unwrap.filepath = location.file(),
            unwrap.lineno = location.line(),
            unwrap.columnno = location.column(),
            "{}: {:?}",
            msg,
            &value
        );
    }

    #[cfg(not(feature = "log-location"))]
    tracing::error!("{}: {:?}", msg, &value);

    #[cfg(feature = "panic-quiet")]
    panic!();
    #[cfg(not(feature = "panic-quiet"))]
    panic!("{}: {:?}", msg, &value);
}
