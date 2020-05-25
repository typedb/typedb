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
 *
 */

package hypergraph.graph.edge;

import hypergraph.graph.util.IID;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.ThingVertex;

/**
 * An edge between two {@code ThingVertex}.
 *
 * This edge can only have a schema of type {@code Schema.Edge.Thing}.
 */
public interface ThingEdge extends Edge<IID.Edge.Thing, Schema.Edge.Thing, ThingVertex> {

    /**
     * Commits the edge to the graph for persistent storage.
     *
     * After committing this edge to the graph, the status of this edges should
     * be {@code persisted}.
     *
     * @param hasAttributeSyncLock that indicates whether you have access to the {@code AttributeSync}
     */
    void commit(boolean hasAttributeSyncLock);
}
