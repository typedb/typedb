/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graql.gremlin.sets;

import grakn.core.graql.gremlin.EquivalentFragmentSet;
import grakn.core.server.session.TransactionOLTP;

import java.util.Collection;

/**
 * Describes an optimisation strategy that can be applied to EquivalentFragmentSets.
 *
 */
@FunctionalInterface
public interface FragmentSetOptimisation {

    /**
     * Apply the optimisation to the given EquivalentFragmentSets using the given Transaction.
     *
     * <p>
     *     The strategy may modify the collection. If it does, it will return {@code true}, otherwise it will return
     *     {@code false}.
     * </p>
     *
     * @param fragmentSets a mutable collection of EquivalentFragmentSets
     * @param tx the Transaction that these EquivalentFragmentSets are going to operate against
     * @return whether {@code fragmentSets} was modified
     */
    boolean apply(Collection<EquivalentFragmentSet> fragmentSets, TransactionOLTP tx);
}
