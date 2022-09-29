/*
 * Copyright (C) 2022 Vaticle
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
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.iid.VertexIID;
import com.vaticle.typedb.core.graph.vertex.AttributeVertex;

import java.time.LocalDateTime;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingRead.INVALID_THING_VERTEX_CASTING;
import static com.vaticle.typedb.core.encoding.Encoding.Status.BUFFERED;
import static com.vaticle.typedb.core.encoding.Encoding.Status.PERSISTED;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.BOOLEAN;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.DATETIME;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.DOUBLE;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.LONG;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.STRING;

public abstract class AttributeVertexImpl {

    public static abstract class Read<VALUE> extends ThingVertexImpl.Read implements AttributeVertex<VALUE> {

        private final VertexIID.Attribute<VALUE> attributeIID;
        private java.lang.Boolean isPersisted;

        Read(ThingGraph graph, VertexIID.Attribute<VALUE> iid) {
            super(graph, iid);
            this.attributeIID = iid;
            this.isPersisted = null;
        }

        public static AttributeVertexImpl.Read<?> of(ThingGraph graph, VertexIID.Attribute<?> iid) {
            Encoding.ValueType<?> valueType = iid.valueType();
            if (BOOLEAN == valueType) return new Boolean(graph, iid.asBoolean());
            else if (LONG == valueType) return new Long(graph, iid.asLong());
            else if (DOUBLE == valueType) return new Double(graph, iid.asDouble());
            else if (STRING == valueType) return new String(graph, iid.asString());
            else if (DATETIME == valueType) return new DateTime(graph, iid.asDateTime());
            assert false;
            return null;
        }

        @Override
        public VertexIID.Attribute<VALUE> iid() {
            return attributeIID;
        }

        @Override
        public Encoding.ValueType<VALUE> valueType() {
            return attributeIID.valueType();
        }

        @Override
        public Encoding.Status status() {
            return isPersisted() ? PERSISTED : BUFFERED;
        }

        private boolean isPersisted() {
            if (isPersisted == null) isPersisted = graph.storage().get(iid) != null;
            return isPersisted;
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

        @Override
        public AttributeVertex.Write<VALUE> toWrite() {
            AttributeVertex.Write<?> writable = graph.convertToWritable(iid());
            assert writable.valueType().equals(valueType());
            return (AttributeVertex.Write<VALUE>) writable;
        }

        @Override
        public boolean isAttribute() {
            return true;
        }

        @Override
        public boolean isBoolean() {
            return false;
        }

        @Override
        public boolean isLong() {
            return false;
        }

        @Override
        public boolean isDouble() {
            return false;
        }

        @Override
        public boolean isString() {
            return false;
        }

        @Override
        public boolean isDateTime() {
            return false;
        }

        @Override
        public AttributeVertex<?> asAttribute() {
            return this;
        }

        @Override
        public AttributeVertexImpl.Write<VALUE> asWrite() {
            throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(AttributeVertex.Write.class));
        }

        @Override
        public AttributeVertexImpl.Read.Boolean asBoolean() {
            throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(Boolean.class));
        }

        @Override
        public AttributeVertexImpl.Read.Long asLong() {
            throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(Long.class));
        }

        @Override
        public AttributeVertexImpl.Read.Double asDouble() {
            throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(Double.class));
        }

        @Override
        public AttributeVertexImpl.Read.String asString() {
            throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(String.class));
        }

        @Override
        public AttributeVertexImpl.Read.DateTime asDateTime() {
            throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(DateTime.class));
        }

        public static class Boolean extends AttributeVertexImpl.Read<java.lang.Boolean> {

            public Boolean(ThingGraph graph, VertexIID.Attribute<java.lang.Boolean> iid) {
                super(graph, iid);
            }

            @Override
            public boolean isBoolean() {
                return true;
            }

            @Override
            public Boolean asBoolean() {
                return this;
            }
        }

        public static class Long extends AttributeVertexImpl.Read<java.lang.Long> {

            public Long(ThingGraph graph, VertexIID.Attribute.Long iid) {
                super(graph, iid);
            }

            @Override
            public boolean isLong() {
                return true;
            }

            @Override
            public Long asLong() {
                return this;
            }
        }

        public static class Double extends AttributeVertexImpl.Read<java.lang.Double> {

            public Double(ThingGraph graph, VertexIID.Attribute.Double iid) {
                super(graph, iid);
            }

            @Override
            public boolean isDouble() {
                return true;
            }

            @Override
            public Double asDouble() {
                return this;
            }
        }

        public static class String extends AttributeVertexImpl.Read<java.lang.String> {

            public String(ThingGraph graph, VertexIID.Attribute.String iid) {
                super(graph, iid);
            }

            @Override
            public boolean isString() {
                return true;
            }

            @Override
            public String asString() {
                return this;
            }
        }

        public static class DateTime extends AttributeVertexImpl.Read<java.time.LocalDateTime> {

            public DateTime(ThingGraph graph, VertexIID.Attribute<LocalDateTime> iid) {
                super(graph, iid);
            }

            @Override
            public boolean isDateTime() {
                return true;
            }

            @Override
            public DateTime asDateTime() {
                return this;
            }
        }

    }

    public static abstract class Write<VALUE> extends ThingVertexImpl.Write implements AttributeVertex.Write<VALUE> {

        private final VertexIID.Attribute<VALUE> attributeIID;
        private final boolean isInferred;
        private java.lang.Boolean isPersisted;

        private Write(ThingGraph graph, VertexIID.Attribute<VALUE> iid, boolean isInferred) {
            super(graph, iid);
            this.attributeIID = iid;
            this.isInferred = isInferred;
            this.isPersisted = null;
        }

        public static AttributeVertexImpl.Write<?> of(ThingGraph graph, VertexIID.Attribute<?> iid) {
            Encoding.ValueType<?> valueType = iid.valueType();
            if (BOOLEAN == valueType) return new Boolean(graph, iid.asBoolean());
            else if (LONG == valueType) return new Long(graph, iid.asLong());
            else if (DOUBLE == valueType) return new Double(graph, iid.asDouble());
            else if (STRING == valueType) return new String(graph, iid.asString());
            else if (DATETIME == valueType) return new DateTime(graph, iid.asDateTime());
            assert false;
            return null;
        }

        @Override
        protected ThingAdjacency.Write.In newInAdjacency() {
            return new ThingAdjacencyImpl.Write.Persisted.In(this);
        }

        @Override
        protected ThingAdjacency.Write.Out newOutAdjacency() {
            return new ThingAdjacencyImpl.Write.Persisted.Out(this);
        }

        @Override
        public VertexIID.Attribute<VALUE> iid() {
            return attributeIID;
        }

        @Override
        public Encoding.ValueType<VALUE> valueType() {
            return attributeIID.valueType();
        }

        @Override
        public Encoding.Status status() {
            return isPersisted() ? PERSISTED : BUFFERED;
        }

        private boolean isPersisted() {
            if (isPersisted == null) isPersisted = graph.storage().get(iid) != null;
            return isPersisted;
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

        @Override
        public boolean isInferred() {
            return isInferred;
        }

        @Override
        public AttributeVertex.Write<VALUE> toWrite() {
            return this;
        }

        @Override
        public void setModified() {
            if (!isModified) {
                isModified = true;
                graph.setModified(iid());
            }
        }

        @Override
        public void delete() {
            if (isDeleted.compareAndSet(false, true)) {
                deleteEdges();
                deleteVertexFromStorage();
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
            commitVertex();
            commitEdges();
        }

        @Override
        public boolean isAttribute() {
            return true;
        }

        @Override
        public boolean isBoolean() {
            return false;
        }

        @Override
        public boolean isLong() {
            return false;
        }

        @Override
        public boolean isDouble() {
            return false;
        }

        @Override
        public boolean isString() {
            return false;
        }

        @Override
        public boolean isDateTime() {
            return false;
        }

        @Override
        public AttributeVertexImpl.Write<?> asAttribute() {
            return this;
        }

        @Override
        public AttributeVertexImpl.Write<VALUE> asWrite() {
            return this;
        }

        @Override
        public AttributeVertexImpl.Write.Boolean asBoolean() {
            throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(Boolean.class));
        }

        @Override
        public AttributeVertexImpl.Write.Long asLong() {
            throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(Long.class));
        }

        @Override
        public AttributeVertexImpl.Write.Double asDouble() {
            throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(Double.class));
        }

        @Override
        public AttributeVertexImpl.Write.String asString() {
            throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(String.class));
        }

        @Override
        public AttributeVertexImpl.Write.DateTime asDateTime() {
            throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(DateTime.class));
        }

        public static class Boolean extends AttributeVertexImpl.Write<java.lang.Boolean> {

            public Boolean(ThingGraph graph, VertexIID.Attribute<java.lang.Boolean> iid) {
                this(graph, iid, false);
            }

            public Boolean(ThingGraph graph, VertexIID.Attribute<java.lang.Boolean> iid, boolean isInferred) {
                super(graph, iid, isInferred);
            }

            @Override
            public boolean isBoolean() {
                return true;
            }

            @Override
            public Boolean asBoolean() {
                return this;
            }
        }

        public static class Long extends AttributeVertexImpl.Write<java.lang.Long> {

            public Long(ThingGraph graph, VertexIID.Attribute.Long iid) {
                this(graph, iid, false);
            }

            public Long(ThingGraph graph, VertexIID.Attribute<java.lang.Long> iid, boolean isInferred) {
                super(graph, iid, isInferred);
            }

            @Override
            public boolean isLong() {
                return true;
            }

            @Override
            public Long asLong() {
                return this;
            }
        }

        public static class Double extends AttributeVertexImpl.Write<java.lang.Double> {

            public Double(ThingGraph graph, VertexIID.Attribute.Double iid) {
                this(graph, iid, false);
            }

            public Double(ThingGraph graph, VertexIID.Attribute<java.lang.Double> iid, boolean isInferred) {
                super(graph, iid, isInferred);
            }

            @Override
            public boolean isDouble() {
                return true;
            }

            @Override
            public Double asDouble() {
                return this;
            }
        }

        public static class String extends AttributeVertexImpl.Write<java.lang.String> {

            public String(ThingGraph graph, VertexIID.Attribute.String iid) {
                this(graph, iid, false);
            }

            public String(ThingGraph graph, VertexIID.Attribute<java.lang.String> iid, boolean isInferred) {
                super(graph, iid, isInferred);
            }

            @Override
            public boolean isString() {
                return true;
            }

            @Override
            public String asString() {
                return this;
            }
        }

        public static class DateTime extends AttributeVertexImpl.Write<java.time.LocalDateTime> {

            public DateTime(ThingGraph graph, VertexIID.Attribute.DateTime iid) {
                this(graph, iid, false);
            }

            public DateTime(ThingGraph graph, VertexIID.Attribute<LocalDateTime> iid, boolean isInferred) {
                super(graph, iid, isInferred);
            }

            @Override
            public boolean isDateTime() {
                return true;
            }

            @Override
            public DateTime asDateTime() {
                return this;
            }
        }
    }
}
