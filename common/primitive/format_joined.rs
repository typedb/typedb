/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

pub struct FormatJoined<'a, Iter>(pub &'a Iter, pub char);

impl<'a, Iter, T> fmt::Display for FormatJoined<'a, Iter>
where
    &'a Iter: IntoIterator<Item = T>,
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut printing_first = true;
        for item in self.0 {
            if printing_first {
                write!(f, "{}", item)?;
                printing_first = false;
            } else {
                write!(f, "{} {}", self.1, item)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::BTreeSet;
    use crate::format_joined::FormatJoined;

    #[test]
    fn test_joined_write() {
        let empty: Vec<usize> = vec![];
        assert_eq!(
            format!("f({})", FormatJoined(&empty, ',')).as_str(),
            "f()"
        );
        assert_eq!(
            format!("f({})", FormatJoined(&[1], ',')).as_str(),
            "f(1)"
        );
        assert_eq!(
            format!("f({})", FormatJoined(&[1, 2, 3], ',')).as_str(),
            "f(1, 2, 3)"
        );

        assert_eq!(
            format!("BTreeSet({})", FormatJoined(&BTreeSet::from(["Bob", "Alice"]), ',')).as_str(),
            "BTreeSet(Alice, Bob)"
        );
    }
}
