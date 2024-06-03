/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

/*
Optimisation pass to convert simple binary relation traversal into rp-index lookups:

Given:
$r roleplayer $x (role filter: $role1)
$r roleplayer $y (role filter: $role2)

Where $r, $role1, and $role2 are NOT returned
Then we can collapse these into:

$x relation-rp-indexed $y (rel filter: $r, role-left filter: $role1, role-right filter: $role2)
 */
