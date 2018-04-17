/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.gremlin.sets;

import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * @see EquivalentFragmentSets#plays(VarProperty, Var, Var, boolean)
 *
 * @author Felix Chapman
 */
@AutoValue
abstract class PlaysFragmentSet extends EquivalentFragmentSet {

    @Override
    public final Set<Fragment> fragments() {
        return ImmutableSet.of(
                Fragments.outPlays(varProperty(), type(), role(), required()),
                Fragments.inPlays(varProperty(), role(), type(), required())
        );
    }

    abstract Var type();
    abstract Var role();
    abstract boolean required();
}
