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

import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;

import static ai.grakn.graql.internal.gremlin.fragment.Fragments.inIsaCastings;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.outIsaCastings;

/**
 * @author Felix Chapman
 */
class IsaCastingsFragmentSet extends EquivalentFragmentSet {

    private final Var casting;
    private final Var roleType;

    IsaCastingsFragmentSet(VarProperty varProperty, Var casting, Var roleType) {
        super(outIsaCastings(varProperty, casting, roleType), inIsaCastings(varProperty, roleType, casting));
        this.casting = casting;
        this.roleType = roleType;
    }

    public Var casting() {
        return casting;
    }

    public Var roleType() {
        return roleType;
    }
}
