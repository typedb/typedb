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
 */

package ai.grakn.graql.internal.pattern.property;

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.query.InsertQueryExecutor;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Internal interface for {@link VarProperty}, providing additional methods to match, insert or define the property.
 *
 * @author Felix Chapman
 */
public interface VarPropertyInternal extends VarProperty {

    /**
     * Check if the given property can be used in a match query
     */
    void checkValid(GraknTx graph, VarPatternAdmin var) throws GraqlQueryException;

    /**
     * Check if the given property can be inserted
     */
    default void checkInsertable(VarPatternAdmin var) throws GraqlQueryException {
    }

    /**
     * Return a collection of {@link EquivalentFragmentSet} to match the given property in the graph
     */
    Collection<EquivalentFragmentSet> match(Var start);

    /**
     * Insert the given property into the graph, if possible.
     *
     * @param var      the subject var of the property
     * @param executor a class providing a map of concepts already inserted and methods to build new concepts.
     *                 <p>
     *                 This method can expect any key to be here that is returned from
     *                 {@link #requiredVars(Var)}. The method may also build a concept provided that key is returned
     *                 from {@link #producedVars(Var)}.
     *                 </p>
     */
    void insert(Var var, InsertQueryExecutor executor) throws GraqlQueryException;

    void define(Var var, InsertQueryExecutor executor) throws GraqlQueryException;

    /**
     * Get all {@link Var}s whose {@link Concept} must exist for the subject {@link Var} to be created.
     * For example, for {@link IsaProperty} the type must already be present before an instance can be created.
     *
     * When calling {@link #insert}, the method can expect any entry returned here to be in the
     * map of concepts.
     *
     * @param var the subject var of the property.
     */
    Set<Var> requiredVars(Var var);

    /**
     * Get all {@link Var}s whose {@link Concept} can only be created after this property is applied.
     *
     * <p>
     *     When calling {@link #insert}, the method must add an entry for every {@link Var} returned
     *     from this method.
     * </p>
     * <p>
     *     The default implementation returns an empty set.
     * </p>
     *
     * @param var the subject var of the property.
     */
    default Set<Var> producedVars(Var var) {
        return ImmutableSet.of();
    }

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
    static VarPropertyInternal from(VarProperty varProperty) {
        return (VarPropertyInternal) varProperty;
    }
}
