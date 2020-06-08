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
import hypergraph.graph.iid.EdgeIID;
import hypergraph.graph.iid.IndexIID;
import hypergraph.graph.iid.VertexIID;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.AttributeVertex;

import java.time.LocalDateTime;

public abstract class AttributeVertexImpl<VALUE> extends ThingVertexImpl implements AttributeVertex<VALUE> {

    private final VertexIID.Attribute<VALUE> attributeIID;

    AttributeVertexImpl(ThingGraph graph, VertexIID.Attribute<VALUE> iid, boolean isInferred) {
        super(graph, iid, isInferred);
        this.attributeIID = iid;
    }

    public static AttributeVertexImpl<?> of(ThingGraph graph, VertexIID.Attribute iid) {
        switch (iid.valueType()) {
            case BOOLEAN:
                return new AttributeVertexImpl.Boolean(graph, iid.asBoolean(), false);
            case LONG:
                return new AttributeVertexImpl.Long(graph, iid.asLong(), false);
            case DOUBLE:
                return new AttributeVertexImpl.Double(graph, iid.asDouble(), false);
            case STRING:
                return new AttributeVertexImpl.String(graph, iid.asString(), false);
            case DATETIME:
                return new AttributeVertexImpl.DateTime(graph, iid.asDateTime(), false);
            default:
                assert false;
                return null;
        }
    }

    protected abstract IndexIID.Attribute index();

    @Override
    protected ThingAdjacency newAdjacency(Adjacency.Direction direction) {
        return new ThingAdjacencyImpl.Persisted(this, direction);
    }

    @Override
    public VertexIID.Attribute<VALUE> iid() {
        return attributeIID;
    }

    @Override
    public Schema.Status status() {
        return Schema.Status.IMMUTABLE;
    }

    @Override
    public VALUE value() {
        if (type().valueType().isIndexable()) {
            return attributeIID.value();
        } else {
            // TODO: implement for ValueType.TEXT
            return null;
        }
    }

    @Override
    public void delete() {
        graph.storage().delete(attributeIID.bytes());
        graph.storage().delete(EdgeIID.InwardsISA.of(type().iid(), iid).bytes());
        graph.storage().delete(index().bytes());
        graph.delete(this);
    }

    /**
     * Commits this vertex to be persisted onto storage.
     *
     * This method is not thread-safe. It uses needs to access and manipulate
     * {@code AttributeSync} which is not a thread-safe object.
     */
    @Override
    public void commit() {
        if (isInferred) throw new HypergraphException(Error.Transaction.ILLEGAL_OPERATION);
        graph.storage().putUntracked(attributeIID.bytes());
        graph.storage().putUntracked(EdgeIID.InwardsISA.of(type().iid(), iid).bytes());
        graph.storage().putUntracked(index().bytes(), attributeIID.bytes());
        outs.forEach(Edge::commit);
        ins.forEach(Edge::commit);
    }

    @Override
    public AttributeVertexImpl.Boolean asBoolean() {
        throw new HypergraphException(Error.ThingRead.INVALID_VERTEX_CASTING.format(Boolean.class.getCanonicalName()));
    }

    @Override
    public AttributeVertexImpl.Long asLong() {
        throw new HypergraphException(Error.ThingRead.INVALID_VERTEX_CASTING.format(Long.class.getCanonicalName()));
    }

    @Override
    public AttributeVertexImpl.Double asDouble() {
        throw new HypergraphException(Error.ThingRead.INVALID_VERTEX_CASTING.format(Double.class.getCanonicalName()));
    }

    @Override
    public AttributeVertexImpl.String asString() {
        throw new HypergraphException(Error.ThingRead.INVALID_VERTEX_CASTING.format(String.class.getCanonicalName()));
    }

    @Override
    public AttributeVertexImpl.DateTime asDateTime() {
        throw new HypergraphException(Error.ThingRead.INVALID_VERTEX_CASTING.format(DateTime.class.getCanonicalName()));
    }

    public static class Boolean extends AttributeVertexImpl<java.lang.Boolean> {

        public Boolean(ThingGraph graph, VertexIID.Attribute<java.lang.Boolean> iid, boolean isInferred) {
            super(graph, iid, isInferred);
        }

        @Override
        protected IndexIID.Attribute index() {
            return IndexIID.Attribute.of(value(), type().iid());
        }

        @Override
        public Boolean asBoolean() {
            return this;
        }
    }

    public static class Long extends AttributeVertexImpl<java.lang.Long> {

        public Long(ThingGraph graph, VertexIID.Attribute<java.lang.Long> iid, boolean isInferred) {
            super(graph, iid, isInferred);
        }

        @Override
        protected IndexIID.Attribute index() {
            return IndexIID.Attribute.of(value(), type().iid());
        }

        @Override
        public Long asLong() {
            return this;
        }
    }

    public static class Double extends AttributeVertexImpl<java.lang.Double> {

        public Double(ThingGraph graph, VertexIID.Attribute<java.lang.Double> iid, boolean isInferred) {
            super(graph, iid, isInferred);
        }

        @Override
        protected IndexIID.Attribute index() {
            return IndexIID.Attribute.of(value(), type().iid());
        }

        @Override
        public Double asDouble() {
            return this;
        }
    }

    public static class String extends AttributeVertexImpl<java.lang.String> {

        public String(ThingGraph graph, VertexIID.Attribute<java.lang.String> iid, boolean isInferred) {
            super(graph, iid, isInferred);
        }

        @Override
        protected IndexIID.Attribute index() {
            return IndexIID.Attribute.of(value(), type().iid());
        }

        @Override
        public String asString() {
            return this;
        }
    }

    public static class DateTime extends AttributeVertexImpl<java.time.LocalDateTime> {

        public DateTime(ThingGraph graph, VertexIID.Attribute<LocalDateTime> iid, boolean isInferred) {
            super(graph, iid, isInferred);
        }

        @Override
        protected IndexIID.Attribute index() {
            return IndexIID.Attribute.of(value(), type().iid());
        }

        @Override
        public DateTime asDateTime() {
            return this;
        }
    }
}
