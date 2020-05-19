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
import hypergraph.graph.edge.Edge;
import hypergraph.graph.edge.TypeEdge;
import hypergraph.graph.util.IID;
import hypergraph.graph.util.KeyGenerator;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.Iterator;

import static hypergraph.common.collection.ByteArrays.join;

public abstract class TypeVertexImpl extends VertexImpl<IID.Vertex.Type, Schema.Vertex.Type, TypeVertex, Schema.Edge.Type, TypeEdge> implements TypeVertex {

    protected final TypeGraph graph;
    protected final TypeAdjacency outs;
    protected final TypeAdjacency ins;

    protected String label;
    protected String scope;
    protected Boolean isAbstract;
    protected Schema.ValueType valueType;
    protected String regex;


    TypeVertexImpl(TypeGraph graph, IID.Vertex.Type iid, String label, @Nullable String scope) {
        super(iid, iid.schema());
        this.graph = graph;
        this.label = label;
        this.scope = scope;
        this.outs = newAdjacency(Adjacency.Direction.OUT);
        this.ins = newAdjacency(Adjacency.Direction.IN);
    }

    /**
     * Generate an IID for a {@code TypeVertex} for a given {@code Schema}
     *
     * @param keyGenerator to generate the IID for a {@code TypeVertex}
     * @param schema       of the {@code TypeVertex} in which the IID will be used for
     * @return a byte array representing a new IID for a {@code TypeVertex}
     */
    public static IID.Vertex.Type generateIID(KeyGenerator keyGenerator, Schema.Vertex.Type schema) {
        return IID.Vertex.Type.of(join(schema.prefix().key(), keyGenerator.forType(IID.Prefix.of(schema.prefix().key()))));
    }

    /**
     * Returns the index address of given {@code TypeVertex}
     *
     * @param label of the {@code TypeVertex}
     * @param scope of the {@code TypeVertex}, which could be null
     * @return a byte array representing the index address of a {@code TypeVertex}
     */
    public static byte[] index(String label, @Nullable String scope) {
        return join(Schema.Index.TYPE.prefix().key(), scopedLabel(label, scope).getBytes());
    }

    /**
     * Returns the fully scoped label for a given {@code TypeVertex}
     *
     * @param label the unscoped label of the {@code TypeVertex}
     * @param scope the scope label of the {@code TypeVertex}
     * @return the fully scoped label for a given {@code TypeVertex} as a string
     */
    public static String scopedLabel(String label, @Nullable String scope) {
        if (scope == null) return label;
        else return scope + ":" + label;
    }

    /**
     * Instantiates a new {@code TypeAdjacency} class
     *
     * @param direction the direction of the edges held in {@code TypeAdjacency}
     * @return the new {@code TypeAdjacency} class
     */
    protected abstract TypeAdjacency newAdjacency(Adjacency.Direction direction);

    /**
     * Get the {@code Graph} containing all {@code TypeVertex}
     *
     * @return the {@code Graph} containing all {@code TypeVertex}
     */
    @Override
    public TypeGraph graph() {
        return graph;
    }

    @Override
    public String label() {
        return label;
    }

    @Override
    public String scopedLabel() {
        return scopedLabel(label, scope);
    }

    @Override
    public TypeAdjacency outs() {
        return outs;
    }

    @Override
    public TypeAdjacency ins() {
        return ins;
    }

    public static class Buffered extends TypeVertexImpl {

        public Buffered(TypeGraph graph, Schema.Vertex.Type schema, IID.Vertex.Type iid, String label, @Nullable String scope) {
            super(graph, iid, label, scope);
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
        protected TypeAdjacency newAdjacency(Adjacency.Direction direction) {
            return new TypeAdjacencyImpl.Buffered(this, direction);
        }

        public Schema.Status status() {
            return Schema.Status.BUFFERED;
        }

        public boolean isAbstract() {
            return isAbstract != null ? isAbstract : false;
        }

        public TypeVertexImpl isAbstract(boolean isAbstract) {
            this.isAbstract = isAbstract;
            return this;
        }

        public Schema.ValueType valueType() {
            return valueType;
        }

        public TypeVertexImpl valueType(Schema.ValueType valueType) {
            this.valueType = valueType;
            return this;
        }

        public String regex() {
            return regex;
        }

        public TypeVertexImpl regex(String regex) {
            this.regex = regex;
            return this;
        }

        @Override
        public void commit() {
            graph.storage().put(iid.bytes());
            commitIndex();
            commitProperties();
            commitEdges();
        }

        @Override
        public void delete() {
            ins.deleteAll();
            outs.deleteAll();
            graph.delete(this);
        }

        void commitIndex() {
            graph.storage().put(index(label, scope), iid.bytes());
        }

        void commitProperties() {
            commitPropertyLabel();
            if (scope != null) commitPropertyScope();
            if (isAbstract != null && isAbstract) commitPropertyAbstract();
            if (valueType != null) commitPropertyValueType();
            if (regex != null && !regex.isEmpty()) commitPropertyRegex();
        }

        private void commitPropertyScope() {
            graph.storage().put(join(iid.bytes(), Schema.Property.SCOPE.infix().key()), scope.getBytes());
        }

        private void commitPropertyAbstract() {
            graph.storage().put(join(iid.bytes(), Schema.Property.ABSTRACT.infix().key()));
        }

        private void commitPropertyLabel() {
            graph.storage().put(join(iid.bytes(), Schema.Property.LABEL.infix().key()), label.getBytes());
        }

        private void commitPropertyValueType() {
            graph.storage().put(join(iid.bytes(), Schema.Property.VALUE_TYPE.infix().key()), valueType.key());
        }

        private void commitPropertyRegex() {
            graph.storage().put(join(iid.bytes(), Schema.Property.REGEX.infix().key()), regex.getBytes());
        }

        private void commitEdges() {
            outs.forEach(Edge::commit);
            ins.forEach(Edge::commit);
        }
    }

    public static class Persisted extends TypeVertexImpl {

        public Persisted(TypeGraph graph, IID.Vertex.Type iid, String label, @Nullable String scope) {
            super(graph, iid, label, scope);
        }

        public Persisted(TypeGraph graph, IID.Vertex.Type iid) {
            super(graph, iid,
                  new String(graph.storage().get(join(iid.bytes(), Schema.Property.LABEL.infix().key()))),
                  getScope(graph, iid));
        }

        @Nullable
        private static String getScope(TypeGraph graph, IID.Vertex.Type iid) {
            byte[] scopeBytes = graph.storage().get(join(iid.bytes(), Schema.Property.SCOPE.infix().key()));
            if (scopeBytes != null) return new String(scopeBytes);
            else return null;
        }

        @Override
        protected TypeAdjacency newAdjacency(Adjacency.Direction direction) {
            return new TypeAdjacencyImpl.Persisted(this, direction);
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
            graph.storage().put(join(iid.bytes(), Schema.Property.LABEL.infix().key()), label.getBytes());
            graph.storage().delete(index(this.label, scope));
            graph.storage().put(index(label, scope), iid.bytes());
            this.label = label;
            return this;
        }

        @Override
        public TypeVertexImpl scope(String scope) {
            graph.update(this, label, this.scope, label, scope);
            graph.storage().put(join(iid.bytes(), Schema.Property.SCOPE.infix().key()), scope.getBytes());
            graph.storage().delete(index(label, this.scope));
            graph.storage().put(index(label, scope), iid.bytes());
            this.scope = scope;
            return this;
        }

        @Override
        public boolean isAbstract() {
            if (isAbstract != null) return isAbstract;
            byte[] flag = graph.storage().get(join(iid.bytes(), Schema.Property.ABSTRACT.infix().key()));
            isAbstract = flag != null;
            return isAbstract;
        }

        @Override
        public TypeVertexImpl isAbstract(boolean isAbstract) {
            if (isAbstract) graph.storage().put(join(iid.bytes(), Schema.Property.ABSTRACT.infix().key()));
            else graph.storage().delete(join(iid.bytes(), Schema.Property.ABSTRACT.infix().key()));
            this.isAbstract = isAbstract;
            return this;
        }

        @Override
        public Schema.ValueType valueType() {
            if (valueType != null) return valueType;
            byte[] val = graph.storage().get(join(iid.bytes(), Schema.Property.VALUE_TYPE.infix().key()));
            if (val != null) valueType = Schema.ValueType.of(val[0]);
            return valueType;
        }

        @Override
        public TypeVertexImpl valueType(Schema.ValueType valueType) {
            graph.storage().put(join(iid.bytes(), Schema.Property.VALUE_TYPE.infix().key()), valueType.key());
            this.valueType = valueType;
            return this;
        }

        @Override
        public String regex() {
            if (regex != null) return regex;
            byte[] val = graph.storage().get(join(iid.bytes(), Schema.Property.REGEX.infix().key()));
            if (val != null) regex = new String(val);
            return regex;
        }

        @Override
        public TypeVertexImpl regex(String regex) {
            graph.storage().put(join(iid.bytes(), Schema.Property.REGEX.infix().key()), regex.getBytes());
            this.regex = regex;
            return this;
        }

        @Override
        public void commit() {
            commitEdges();
        }

        @Override
        public void delete() {
            ins.deleteAll();
            outs.deleteAll();
            graph.delete(this);
            graph.storage().delete(index(label, scope));
            Iterator<byte[]> keys = graph.storage().iterate(iid.bytes(), (iid, value) -> iid);
            while (keys.hasNext()) graph.storage().delete(keys.next());
        }

        private void commitEdges() {
            outs.forEach(Edge::commit);
            ins.forEach(Edge::commit);
        }

    }
}
