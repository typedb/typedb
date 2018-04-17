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

import ai.grakn.GraknTx;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;

import java.util.Collection;

/**
 * Describes an optimisation strategy that can be applied to {@link EquivalentFragmentSet}s.
 *
 * @author Felix Chapman
 */
@FunctionalInterface
public interface FragmentSetOptimisation {

    /**
     * Apply the optimisation to the given {@link EquivalentFragmentSet}s using the given {@link GraknTx}.
     *
     * <p>
     *     The strategy may modify the collection. If it does, it will return {@code true}, otherwise it will return
     *     {@code false}.
     * </p>
     *
     * @param fragmentSets a mutable collection of {@link EquivalentFragmentSet}s
     * @param tx the {@link GraknTx} that these {@link EquivalentFragmentSet}s are going to operate against
     * @return whether {@code fragmentSets} was modified
     */
    boolean apply(Collection<EquivalentFragmentSet> fragmentSets, GraknTx tx);
}
