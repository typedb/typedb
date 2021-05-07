/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.graph.vertex.impl;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.graph.ThingGraph;
import com.vaticle.typedb.core.graph.adjacency.ThingAdjacency;
import com.vaticle.typedb.core.graph.adjacency.impl.ThingAdjacencyImpl;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.iid.EdgeIID;
import com.vaticle.typedb.core.graph.iid.IndexIID;
import com.vaticle.typedb.core.graph.iid.VertexIID;
import com.vaticle.typedb.core.graph.vertex.AttributeVertex;

import java.time.LocalDateTime;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingRead.INVALID_THING_VERTEX_CASTING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.ILLEGAL_OPERATION;

public abstract class AttributeVertexImpl<VALUE> extends ThingVertexImpl implements AttributeVertex<VALUE> {

    private final VertexIID.Attribute<VALUE> attributeIID;
    private java.lang.Boolean isPersisted;

    AttributeVertexImpl(ThingGraph graph, VertexIID.Attribute<VALUE> iid, boolean isInferred) {
        super(graph, iid, isInferred);
        this.attributeIID = iid;
        this.isPersisted = null;
    }

    public static AttributeVertexImpl<?> of(ThingGraph graph, VertexIID.Attribute<?> iid) {
        switch (iid.valueType()) {
            case BOOLEAN:
                return new AttributeVertexImpl.Boolean(graph, iid.asBoolean());
            case LONG:
                return new AttributeVertexImpl.Long(graph, iid.asLong());
            case DOUBLE:
                return new AttributeVertexImpl.Double(graph, iid.asDouble());
            case STRING:
                return new AttributeVertexImpl.String(graph, iid.asString());
            case DATETIME:
                return new AttributeVertexImpl.DateTime(graph, iid.asDateTime());
            default:
                assert false;
                return null;
        }
    }

    protected abstract IndexIID.Attribute index();

    @Override
    protected ThingAdjacency newAdjacency(Encoding.Direction.Adjacency direction) {
        return new ThingAdjacencyImpl.Persisted(this, direction);
    }

    @Override
    public VertexIID.Attribute<VALUE> iid() {
        return attributeIID;
    }

    @Override
    public Encoding.ValueType valueType() {
        return attributeIID.valueType();
    }

    @Override
    public Encoding.Status status() {
        return Encoding.Status.IMMUTABLE;
    }

    @Override
    public VALUE value() {
        if (type().valueType().isWritable()) {
            return attributeIID.value();
        } else {
            // TODO: implement for ValueType.TEXT
            return null;
        }
    }

    void deleteVertexFromIndex() {
        graph.storage().delete(index().bytes());
    }

    @Override
    void deleteVertexFromGraph() {
        graph.delete(this);
    }

    @Override
    public void delete() {
        if (isDeleted.compareAndSet(false, true)) {
            deleteEdges();
            deleteVertexFromStorage();
            deleteVertexFromIndex();
            deleteVertexFromGraph();
        }
    }

    /**
     * Commits this vertex to be persisted onto storage.
     *
     * This method is not thread-safe. It uses needs to access and manipulate
     * {@code AttributeSync} which is not a thread-safe object.
     */
    @Override
    public void commit() {
        if (isInferred) throw TypeDBException.of(ILLEGAL_OPERATION);
        commitVertex();
        commitEdges();
    }

    private void commitVertex() {
        if (!isPersisted()) {
            graph.storage().put(attributeIID.bytes());
            graph.storage().put(EdgeIID.InwardsISA.of(type().iid(), iid).bytes());
            graph.storage().put(index().bytes(), attributeIID.bytes());
        } else {
            setModified();
        }
        // TODO: we should make use of attribute indexes to look up attributes by value (without type) quickly
    }

    @Override
    public void setModified() {
        if (!isModified) {
            isModified = true;
            if (isPersisted()) graph.setModified(iid);
        }
    }

    private boolean isPersisted() {
        if (isPersisted == null) isPersisted = graph.storage().get(iid.bytes()) != null;
        return isPersisted;
    }

    @Override
    public boolean isAttribute() { return true; }

    @Override
    public boolean isBoolean() { return false; }

    @Override
    public boolean isLong() { return false; }

    @Override
    public boolean isDouble() { return false; }

    @Override
    public boolean isString() { return false; }

    @Override
    public boolean isDateTime() { return false; }

    @Override
    public AttributeVertexImpl<?> asAttribute() { return this; }

    @Override
    public AttributeVertexImpl.Boolean asBoolean() {
        throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(Boolean.class));
    }

    @Override
    public AttributeVertexImpl.Long asLong() {
        throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(Long.class));
    }

    @Override
    public AttributeVertexImpl.Double asDouble() {
        throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(Double.class));
    }

    @Override
    public AttributeVertexImpl.String asString() {
        throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(String.class));
    }

    @Override
    public AttributeVertexImpl.DateTime asDateTime() {
        throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(DateTime.class));
    }

    public static class Boolean extends AttributeVertexImpl<java.lang.Boolean> {

        public Boolean(ThingGraph graph, VertexIID.Attribute<java.lang.Boolean> iid) {
            this(graph, iid, false);
        }

        public Boolean(ThingGraph graph, VertexIID.Attribute<java.lang.Boolean> iid, boolean isInferred) {
            super(graph, iid, isInferred);
        }

        @Override
        protected IndexIID.Attribute index() {
            return IndexIID.Attribute.of(value(), type().iid());
        }

        @Override
        public boolean isBoolean() { return true; }

        @Override
        public Boolean asBoolean() { return this; }
    }

    public static class Long extends AttributeVertexImpl<java.lang.Long> {

        public Long(ThingGraph graph, VertexIID.Attribute.Long iid) {
            this(graph, iid, false);
        }

        public Long(ThingGraph graph, VertexIID.Attribute<java.lang.Long> iid, boolean isInferred) {
            super(graph, iid, isInferred);
        }

        @Override
        protected IndexIID.Attribute index() {
            return IndexIID.Attribute.of(value(), type().iid());
        }

        @Override
        public boolean isLong() { return true; }

        @Override
        public Long asLong() { return this; }
    }

    public static class Double extends AttributeVertexImpl<java.lang.Double> {

        public Double(ThingGraph graph, VertexIID.Attribute.Double iid) {
            this(graph, iid, false);
        }

        public Double(ThingGraph graph, VertexIID.Attribute<java.lang.Double> iid, boolean isInferred) {
            super(graph, iid, isInferred);
        }

        @Override
        protected IndexIID.Attribute index() {
            return IndexIID.Attribute.of(value(), type().iid());
        }

        @Override
        public boolean isDouble() { return true; }

        @Override
        public Double asDouble() { return this; }
    }

    public static class String extends AttributeVertexImpl<java.lang.String> {

        public String(ThingGraph graph, VertexIID.Attribute.String iid) {
            this(graph, iid, false);
        }

        public String(ThingGraph graph, VertexIID.Attribute<java.lang.String> iid, boolean isInferred) {
            super(graph, iid, isInferred);
        }

        @Override
        protected IndexIID.Attribute index() {
            return IndexIID.Attribute.of(value(), type().iid());
        }

        @Override
        public boolean isString() { return true; }

        @Override
        public String asString() { return this; }
    }

    public static class DateTime extends AttributeVertexImpl<java.time.LocalDateTime> {

        public DateTime(ThingGraph graph, VertexIID.Attribute.DateTime iid) {
            this(graph, iid, false);
        }

        public DateTime(ThingGraph graph, VertexIID.Attribute<LocalDateTime> iid, boolean isInferred) {
            super(graph, iid, isInferred);
        }

        @Override
        protected IndexIID.Attribute index() {
            return IndexIID.Attribute.of(value(), type().iid());
        }

        @Override
        public boolean isDateTime() { return true; }

        @Override
        public DateTime asDateTime() { return this; }
    }
}
