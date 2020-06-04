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
import hypergraph.graph.TypeGraph;
import hypergraph.graph.adjacency.Adjacency;
import hypergraph.graph.adjacency.TypeAdjacency;
import hypergraph.graph.adjacency.impl.TypeAdjacencyImpl;
import hypergraph.graph.edge.TypeEdge;
import hypergraph.graph.util.IID;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.ThingVertex;
import hypergraph.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static hypergraph.common.collection.Bytes.join;

public abstract class TypeVertexImpl extends VertexImpl<IID.Vertex.Type> implements TypeVertex {

    protected final TypeGraph graph;
    protected final TypeAdjacency outs;
    protected final TypeAdjacency ins;

    String label;
    String scope;
    Boolean isAbstract;
    Schema.ValueType valueType;
    String regex;


    TypeVertexImpl(TypeGraph graph, IID.Vertex.Type iid, String label, @Nullable String scope) {
        super(iid);
        this.graph = graph;
        this.label = label;
        this.scope = scope;
        this.outs = newAdjacency(Adjacency.Direction.OUT);
        this.ins = newAdjacency(Adjacency.Direction.IN);
    }

    /**
     * Instantiates a new {@code TypeAdjacency} class
     *
     * @param direction the direction of the edges held in {@code TypeAdjacency}
     * @return the new {@code TypeAdjacency} class
     */
    protected abstract TypeAdjacency newAdjacency(Adjacency.Direction direction);


    @Override
    public TypeGraph graph() {
        return graph;
    }

    @Override
    public Schema.Vertex.Type schema() {
        return iid.schema();
    }

    @Override
    public TypeAdjacency outs() {
        return outs;
    }

    @Override
    public TypeAdjacency ins() {
        return ins;
    }

    @Override
    public void written() {
        graph.written();
    }

    @Override
    public String label() {
        return label;
    }

    @Override
    public String scopedLabel() {
        return Schema.Vertex.Type.scopedLabel(label, scope);
    }

    public static class Buffered extends TypeVertexImpl {

        private final AtomicBoolean committed;

        public Buffered(TypeGraph graph, IID.Vertex.Type iid, String label, @Nullable String scope) {
            super(graph, iid, label, scope);
            this.committed = new AtomicBoolean(false);
            written();
        }

        @Override
        protected TypeAdjacency newAdjacency(Adjacency.Direction direction) {
            return new TypeAdjacencyImpl.Buffered(this, direction);
        }

        @Override
        public Iterator<? extends ThingVertex> instances() {
            return Collections.emptyIterator();
        }

        @Override
        public TypeVertexImpl label(String label) {
            graph.update(this, this.label, scope, label, scope);
            this.label = label;
            return this;
        }

        @Override
        public TypeVertexImpl scope(String scope) {
            graph.update(this, label, this.scope, label, scope);
            this.scope = scope;
            return this;
        }

        @Override
        public Schema.Status status() {
            return committed.get() ? Schema.Status.COMMITTED : Schema.Status.BUFFERED;
        }

        @Override
        public boolean isAbstract() {
            return isAbstract != null ? isAbstract : false;
        }

        @Override
        public TypeVertexImpl isAbstract(boolean isAbstract) {
            this.isAbstract = isAbstract;
            return this;
        }

        @Override
        public Schema.ValueType valueType() {
            return valueType;
        }

        @Override
        public TypeVertexImpl valueType(Schema.ValueType valueType) {
            this.valueType = valueType;
            return this;
        }

        @Override
        public String regex() {
            return regex;
        }

        @Override
        public TypeVertexImpl regex(String regex) {
            this.regex = regex;
            return this;
        }

        @Override
        public void delete() {
            ins.deleteAll();
            outs.deleteAll();
            graph.delete(this);
        }

        @Override
        public void commit() {
            if (committed.compareAndSet(false, true)) {
                graph.storage().put(iid.bytes());
                commitIndex();
                commitProperties();
                commitEdges();
            }
        }

        void commitIndex() {
            graph.storage().put(IID.Index.Type.of(label, scope).bytes(), iid.bytes());
        }

        void commitProperties() {
            commitPropertyLabel();
            if (scope != null) commitPropertyScope();
            if (isAbstract != null && isAbstract) commitPropertyAbstract();
            if (valueType != null) commitPropertyValueType();
            if (regex != null && !regex.isEmpty()) commitPropertyRegex();
        }

        private void commitPropertyScope() {
            graph.storage().put(join(iid.bytes(), Schema.Property.SCOPE.infix().bytes()), scope.getBytes());
        }

        private void commitPropertyAbstract() {
            graph.storage().put(join(iid.bytes(), Schema.Property.ABSTRACT.infix().bytes()));
        }

        private void commitPropertyLabel() {
            graph.storage().put(join(iid.bytes(), Schema.Property.LABEL.infix().bytes()), label.getBytes());
        }

        private void commitPropertyValueType() {
            graph.storage().put(join(iid.bytes(), Schema.Property.VALUE_TYPE.infix().bytes()), valueType.bytes());
        }

        private void commitPropertyRegex() {
            graph.storage().put(join(iid.bytes(), Schema.Property.REGEX.infix().bytes()), regex.getBytes());
        }

        private void commitEdges() {
            outs.forEach(TypeEdge::commit);
            ins.forEach(TypeEdge::commit);
        }
    }

    public static class Persisted extends TypeVertexImpl {

        public Persisted(TypeGraph graph, IID.Vertex.Type iid, String label, @Nullable String scope) {
            super(graph, iid, label, scope);
        }

        public Persisted(TypeGraph graph, IID.Vertex.Type iid) {
            super(graph, iid,
                  new String(graph.storage().get(join(iid.bytes(), Schema.Property.LABEL.infix().bytes()))),
                  getScope(graph, iid));
        }

        @Nullable
        private static String getScope(TypeGraph graph, IID.Vertex.Type iid) {
            byte[] scopeBytes = graph.storage().get(join(iid.bytes(), Schema.Property.SCOPE.infix().bytes()));
            if (scopeBytes != null) return new String(scopeBytes);
            else return null;
        }

        @Override
        protected TypeAdjacency newAdjacency(Adjacency.Direction direction) {
            return new TypeAdjacencyImpl.Persisted(this, direction);
        }

        @Override
        public Iterator<? extends ThingVertex> instances() {
            return null; // TODO
        }

        @Override
        public void iid(IID.Vertex.Type iid) {
            throw new HypergraphException(Error.Transaction.ILLEGAL_OPERATION);
        }

        @Override
        public Schema.Status status() {
            return Schema.Status.PERSISTED;
        }

        @Override
        public TypeVertexImpl label(String label) {
            graph.update(this, this.label, scope, label, scope);
            graph.storage().put(join(iid.bytes(), Schema.Property.LABEL.infix().bytes()), label.getBytes());
            graph.storage().delete(IID.Index.Type.of(this.label, scope).bytes());
            graph.storage().put(IID.Index.Type.of(label, scope).bytes(), iid.bytes());
            this.label = label;
            return this;
        }

        @Override
        public TypeVertexImpl scope(String scope) {
            graph.update(this, label, this.scope, label, scope);
            graph.storage().put(join(iid.bytes(), Schema.Property.SCOPE.infix().bytes()), scope.getBytes());
            graph.storage().delete(IID.Index.Type.of(label, this.scope).bytes());
            graph.storage().put(IID.Index.Type.of(label, scope).bytes(), iid.bytes());
            this.scope = scope;
            return this;
        }

        @Override
        public boolean isAbstract() {
            if (isAbstract != null) return isAbstract;
            byte[] flag = graph.storage().get(join(iid.bytes(), Schema.Property.ABSTRACT.infix().bytes()));
            isAbstract = flag != null;
            return isAbstract;
        }

        @Override
        public TypeVertexImpl isAbstract(boolean isAbstract) {
            if (isAbstract) graph.storage().put(join(iid.bytes(), Schema.Property.ABSTRACT.infix().bytes()));
            else graph.storage().delete(join(iid.bytes(), Schema.Property.ABSTRACT.infix().bytes()));
            this.isAbstract = isAbstract;
            return this;
        }

        @Override
        public Schema.ValueType valueType() {
            if (valueType != null) return valueType;
            byte[] val = graph.storage().get(join(iid.bytes(), Schema.Property.VALUE_TYPE.infix().bytes()));
            if (val != null) valueType = Schema.ValueType.of(val[0]);
            return valueType;
        }

        @Override
        public TypeVertexImpl valueType(Schema.ValueType valueType) {
            graph.storage().put(join(iid.bytes(), Schema.Property.VALUE_TYPE.infix().bytes()), valueType.bytes());
            this.valueType = valueType;
            return this;
        }

        @Override
        public String regex() {
            if (regex != null) return regex;
            byte[] val = graph.storage().get(join(iid.bytes(), Schema.Property.REGEX.infix().bytes()));
            if (val != null) regex = new String(val);
            return regex;
        }

        @Override
        public TypeVertexImpl regex(String regex) {
            graph.storage().put(join(iid.bytes(), Schema.Property.REGEX.infix().bytes()), regex.getBytes());
            this.regex = regex;
            return this;
        }

        @Override
        public void delete() {
            ins.deleteAll();
            outs.deleteAll();
            graph.delete(this);
            graph.storage().delete(IID.Index.Type.of(label, scope).bytes());
            Iterator<byte[]> keys = graph.storage().iterate(iid.bytes(), (iid, value) -> iid);
            while (keys.hasNext()) graph.storage().delete(keys.next());
        }

        @Override
        public void commit() {
            outs.forEach(TypeEdge::commit);
            ins.forEach(TypeEdge::commit);
        }
    }
}
