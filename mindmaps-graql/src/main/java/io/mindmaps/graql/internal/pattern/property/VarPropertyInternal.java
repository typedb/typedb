/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.pattern.property;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.admin.VarProperty;
import io.mindmaps.graql.internal.gremlin.MultiTraversal;
import io.mindmaps.graql.internal.gremlin.ShortcutTraversal;
import io.mindmaps.graql.internal.query.InsertQueryExecutor;

import java.util.Collection;
import java.util.stream.Stream;

public interface VarPropertyInternal extends VarProperty {

    default void modifyShortcutTraversal(ShortcutTraversal shortcutTraversal) {
        shortcutTraversal.setInvalid();
    }

    Collection<MultiTraversal> getMultiTraversals(String start);

    /**
     * Insert the given property into the graph, if possible.
     * @param insertQueryExecutor the instance handling the insert query
     * @param concept the concept to insert a property on
     */
    void insertProperty(InsertQueryExecutor insertQueryExecutor, Concept concept) throws IllegalStateException;

    /**
     * Delete the given property from the graph, if possible.
     * @param graph the graph to operate on
     * @param concept the concept to delete properties of
     */
    void deleteProperty(MindmapsGraph graph, Concept concept) throws IllegalStateException;

    @Override
    default Stream<VarAdmin> getInnerVars() {
        return Stream.empty();
    }
}
