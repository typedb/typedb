/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

/*
We want to determine the output value type of each Expression ( -> Comparisons )

We decided that Expressions and Comparisons are not 'constraining', eg `$x + 4` does not enforce value type constraints on $x to be convertible to long.
 */
