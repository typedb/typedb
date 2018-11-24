/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.graql.query.pattern.property;

import grakn.core.server.Transaction;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.query.Match;
import grakn.core.graql.query.pattern.Var;
import grakn.core.graql.query.pattern.VarPatternAdmin;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * Internal interface for {@link VarProperty}, providing additional methods to match, insert or define the property.
 *
 */
public interface VarInternal extends VarProperty {

    /**
     * Check if the given property can be used in a {@link Match}
     */
    void checkValid(Transaction graph, VarPatternAdmin var) throws GraqlQueryException;

    /**
     * Return a collection of {@link EquivalentFragmentSet} to match the given property in the graph
     */
    Collection<EquivalentFragmentSet> match(Var start);

    /**
     * Returns a {@link Executor} that describes how to insert the given {@link VarProperty} into.
     *
     * @throws GraqlQueryException if this {@link VarProperty} cannot be inserted
     */
    Collection<Executor> insert(Var var) throws GraqlQueryException;

    Collection<Executor> define(Var var) throws GraqlQueryException;

    Collection<Executor> undefine(Var var) throws GraqlQueryException;

    /**
     * Whether this property will uniquely identify a concept in the graph, if one exists.
     * This is used for recognising equivalent variables in insert queries.
     */
    default boolean uniquelyIdentifiesConcept() {
        return false;
    }

    @Override
    default Stream<VarPatternAdmin> innerVarPatterns() {
        return Stream.empty();
    }

    /**
     * Helper method to perform the safe cast into this internal type
     */
    static VarInternal from(VarProperty varProperty) {
        return (VarInternal) varProperty;
    }
}
