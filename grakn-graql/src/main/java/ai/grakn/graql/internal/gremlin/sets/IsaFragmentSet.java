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

import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;

/**
 * @author Felix Chapman
 */
class IsaFragmentSet extends EquivalentFragmentSet {

    private final VarName instance;
    private final VarName type;

    IsaFragmentSet(VarName instance, VarName type) {
        super(Fragments.outIsa(instance, type), Fragments.inIsa(type, instance));
        this.instance = instance;
        this.type = type;
    }

    VarName instance() {
        return instance;
    }

    VarName type() {
        return type;
    }
}
