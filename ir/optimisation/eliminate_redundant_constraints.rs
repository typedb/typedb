/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

/*
Optimisation to remove redundant constraints.

Due to type inference, we should never need to check:

$x type <label>;

since $x will only be allowed take types indicated by the label.

We can also eliminate `isa` constraints, where the type is not named or is named but has no further constraints on it: TODO: double check these preconditions

$x isa $t

Since this will have been taken into account by type inference.

 */
