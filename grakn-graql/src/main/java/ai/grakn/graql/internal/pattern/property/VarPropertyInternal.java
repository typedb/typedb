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

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.ShortcutTraversal;
import ai.grakn.graql.internal.query.InsertQueryExecutor;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * Internal interface for {@link VarProperty}, providing additional methods to match, insert or delete the property.
 *
 * @author Felix Chapman
 */
public interface VarPropertyInternal extends VarProperty {

    default void modifyShortcutTraversal(ShortcutTraversal shortcutTraversal) {
        shortcutTraversal.setInvalid();
    }

    /**
     * Check if the given property can be used in a match query
     */
    void checkValid(GraknGraph graph, VarAdmin var) throws IllegalStateException;

    /**
     * Check if the given property can be inserted
     */
    default void checkInsertable(VarAdmin var) throws IllegalStateException {
    }

    /**
     * Return a collection of {@link EquivalentFragmentSet} to match the given property in the graph
     */
    Collection<EquivalentFragmentSet> match(VarName start);

    /**
     * Insert the given property into the graph, if possible.
     * @param insertQueryExecutor the instance handling the insert query
     * @param concept the concept to insert a property on
     */
    void insert(InsertQueryExecutor insertQueryExecutor, Concept concept) throws IllegalStateException;

    /**
     * Delete the given property from the graph, if possible.
     * @param graph the graph to operate on
     * @param concept the concept to delete properties of
     */
    void delete(GraknGraph graph, Concept concept) throws IllegalStateException;

    @Override
    default Stream<VarAdmin> getInnerVars() {
        return Stream.empty();
    }
}
