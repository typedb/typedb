/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.graph.graphdb.internal;

import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.diskstorage.EntryList;
import grakn.core.graph.diskstorage.keycolumnvalue.SliceQuery;
import grakn.core.graph.graphdb.query.vertex.VertexCentricQueryBuilder;
import grakn.core.graph.util.datastructures.Retriever;

import java.util.List;
import java.util.function.Predicate;

/**
 * Internal Vertex interface adding methods that should only be used by JanusGraph
 *
 */
public interface InternalVertex extends JanusGraphVertex, InternalElement {

    @Override
    InternalVertex it();

    /**
     * Deleted relation e from the adjacency list of this vertex and updates the state of the vertex to reflect
     * the modification.
     * Note that this method tolerates the prior removal of the vertex and hence does not throw an exception
     * if the relation could not actually be removed from the adjacency list. This behavior was chosen to allow
     * relation deletion while iterating over the list of adjacent relations, in which case the relation deletion is taken
     * care of by the iterator and only vertex status update needs to be executed.
     *
     * @param e JanusGraphRelation to be removed
     */
    void removeRelation(InternalRelation e);

    /**
     * Add a new relation to the vertex
     */
    boolean addRelation(InternalRelation e);

    /**
     * Returns an iterable over all newly added relations incident on this vertex that match the given predicate
     */
    List<InternalRelation> getAddedRelations(Predicate<InternalRelation> query);

    /**
     * Returns all relations that match the given query. If these matching relations are not currently
     * held in memory, it uses the given Retriever to retrieve the edges from backend storage.
     */
    EntryList loadRelations(SliceQuery query, Retriever<SliceQuery, EntryList> lookup);

    /**
     * Returns true if the results for the given query have already been loaded for this vertex and are locally cached.
     */
    boolean hasLoadedRelations(SliceQuery query);

    /**
     * Whether this vertex has removed relations
     */
    boolean hasRemovedRelations();

    /**
     * Whether this vertex has added relations
     */
    boolean hasAddedRelations();

    @Override
    VertexCentricQueryBuilder query();


}
