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

package hypergraph.graph.vertex.impl;

import hypergraph.common.exception.Error;
import hypergraph.common.exception.HypergraphException;
import hypergraph.graph.ThingGraph;
import hypergraph.graph.adjacency.Adjacency;
import hypergraph.graph.adjacency.ThingAdjacency;
import hypergraph.graph.adjacency.impl.ThingAdjacencyImpl;
import hypergraph.graph.edge.Edge;
import hypergraph.graph.util.AttributeSync;
import hypergraph.graph.util.IID;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.AttributeVertex;

public class AttributeVertexImpl<VALUE> extends ThingVertexImpl implements AttributeVertex<VALUE> {

    private final IID.Vertex.Attribute<VALUE> attributeIID;
    private AttributeSync.CommitSync commitSync;

    public AttributeVertexImpl(ThingGraph graph, IID.Vertex.Attribute<VALUE> iid, boolean isInferred) {
        super(graph, iid, isInferred);
        this.attributeIID = iid;
    }

    @Override
    protected ThingAdjacency newAdjacency(Adjacency.Direction direction) {
        return new ThingAdjacencyImpl.Persisted(this, direction);
    }

    @Override
    public Schema.Status status() {
        return Schema.Status.IMMUTABLE;
    }

    @Override
    public void isInferred(boolean isInferred) {
        this.isInferred = isInferred;
    }

    @Override
    public VALUE value() {
        if (typeVertex().valueType().isIndexable()) {
            return attributeIID.value();
        } else {
            // TODO: implement for ValueType.TEXT
            return null;
        }
    }

    @Override
    public void commit() {
        if (isInferred) throw new HypergraphException(Error.Transaction.ILLEGAL_OPERATION);
        commitSync = graph.storage().attributeSync().get(attributeIID);
        if (!commitSync.checkIsSyncedAndSetTrue()) {
            graph.storage().put(attributeIID.bytes());
            commitIndex();
        }
        commitEdges();
    }

    private void commitIndex() {

    }

    private void commitEdges() {
        outs.forEach(Edge::commit);
        ins.forEach(Edge::commit);
    }

    @Override
    public void delete() {

    }
}
