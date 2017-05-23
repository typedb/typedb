/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 */

package ai.grakn.graql.internal.gremlin.sets;

import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.Var;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;

import java.util.Optional;
import java.util.Set;

/**
 * Describes the edge connecting a relation to a role-player.
 * <p>
 * Can be constrained with information about the possible role types or relation types.
 *
 * @author Felix Chapman
 */
class ShortcutFragmentSet extends EquivalentFragmentSet {

    ShortcutFragmentSet(
            Var relation, Var edge, Var rolePlayer, Optional<Var> roleType,
            Optional<Set<TypeLabel>> roleTypeLabels, Optional<Set<TypeLabel>> relationTypeLabels) {
        super(
                Fragments.inShortcut(rolePlayer, edge, relation, roleType, roleTypeLabels, relationTypeLabels),
                Fragments.outShortcut(relation, edge, rolePlayer, roleType, roleTypeLabels, relationTypeLabels)
        );
    }
}
