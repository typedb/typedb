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

package hypergraph.graph.vertex;

import hypergraph.graph.adjacency.Adjacency;
import hypergraph.graph.adjacency.TypeAdjacencyImpl;
import hypergraph.graph.adjacency.impl.AdjacencyImpl;
import hypergraph.graph.util.KeyGenerator;
import hypergraph.graph.util.Schema;
import hypergraph.graph.TypeGraph;
import hypergraph.graph.edge.Edge;
import hypergraph.graph.edge.TypeEdge;

import javax.annotation.Nullable;
import java.util.Iterator;

import static hypergraph.common.collection.ByteArrays.join;

public abstract class TypeVertex extends Vertex<Schema.Vertex.Type, TypeVertex, Schema.Edge.Type, TypeEdge> {

    protected final TypeGraph graph;
    protected final TypeAdjacencyImpl outs;
    protected final TypeAdjacencyImpl ins;

    protected String label;
    protected String scope;
    protected Boolean isAbstract;
    protected Schema.ValueClass valueClass;
    protected String regex;


    TypeVertex(TypeGraph graph, Schema.Vertex.Type schema, byte[] iid, String label, @Nullable String scope) {
        super(iid, schema);
        this.graph = graph;
        this.label = label;
        this.scope = scope;
        this.outs = newAdjacency(this, Adjacency.Direction.OUT);
        this.ins = newAdjacency(this, Adjacency.Direction.IN);
    }

    protected abstract TypeAdjacencyImpl newAdjacency(TypeVertex owner, AdjacencyImpl.Direction direction);

    public static String scopedLabel(String label, @Nullable String scope) {
        if (scope == null) return label;
        else return scope + ":" + label;
    }

    /**
     * Get the index address of given {@code TypeVertex}
     *
     * @param label of the {@code TypeVertex}
     * @param scope of the {@code TypeVertex}, which could be null
     * @return a byte array representing the index address of a {@code TypeVertex}
     */
    public static byte[] index(String label, @Nullable String scope) {
        return join(Schema.Index.TYPE.prefix().key(), scopedLabel(label, scope).getBytes());
    }

    /**
     * Generate an IID for a {@code TypeVertex} for a given {@code Schema}
     *
     * @param keyGenerator to generate the IID for a {@code TypeVertex}
     * @param schema       of the {@code TypeVertex} in which the IID will be used for
     * @return a byte array representing a new IID for a {@code TypeVertex}
     */
    public static byte[] generateIID(KeyGenerator keyGenerator, Schema.Vertex.Type schema) {
        return join(schema.prefix().key(), keyGenerator.forType(schema.prefix().key()));
    }

    /**
     * Get the {@code Graph} containing all {@code TypeVertex}
     *
     * @return the {@code Graph} containing all {@code TypeVertex}
     */
    public TypeGraph graph() {
        return graph;
    }

    public String label() {
        return label;
    }

    public String scopedLabel() {
        return scopedLabel(label, scope);
    }

    public TypeAdjacencyImpl outs() {
        return outs;
    }

    public TypeAdjacencyImpl ins() {
        return ins;
    }

    public abstract TypeVertex label(String label);

    public abstract TypeVertex scope(String scope);

    public abstract boolean isAbstract();

    public abstract TypeVertex isAbstract(boolean isAbstract);

    public abstract Schema.ValueClass valueClass();

    public abstract TypeVertex valueClass(Schema.ValueClass valueClass);

    public abstract String regex();

    public abstract TypeVertex regex(String regex);

    public static class Buffered extends TypeVertex {

        public Buffered(TypeGraph graph, Schema.Vertex.Type schema, byte[] iid, String label, @Nullable String scope) {
            super(graph, schema, iid, label, scope);
        }

        @Override
        public TypeVertex label(String label) {
            graph.update(this, this.label, scope, label, scope);
            this.label = label;
            return this;
        }

        @Override
        public TypeVertex scope(String scope) {
            graph.update(this, label, this.scope, label, scope);
            this.scope = scope;
            return this;
        }

        @Override
        protected TypeAdjacencyImpl newAdjacency(TypeVertex owner, AdjacencyImpl.Direction direction) {
            return new TypeAdjacencyImpl.Buffered(owner, direction);
        }

        public Schema.Status status() {
            return Schema.Status.BUFFERED;
        }

        public boolean isAbstract() {
            return isAbstract != null ? isAbstract : false;
        }

        public TypeVertex isAbstract(boolean isAbstract) {
            this.isAbstract = isAbstract;
            return this;
        }

        public Schema.ValueClass valueClass() {
            return valueClass;
        }

        public TypeVertex valueClass(Schema.ValueClass valueClass) {
            this.valueClass = valueClass;
            return this;
        }

        public String regex() {
            return regex;
        }

        public TypeVertex regex(String regex) {
            this.regex = regex;
            return this;
        }

        @Override
        public void commit() {
            graph.storage().put(iid);
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
            graph.storage().put(index(label, scope), iid);
        }

        void commitProperties() {
            commitPropertyLabel();
            if (scope != null) commitPropertyScope();
            if (isAbstract != null && isAbstract) commitPropertyAbstract();
            if (valueClass != null) commitPropertyValueClass();
            if (regex != null && !regex.isEmpty()) commitPropertyRegex();
        }

        private void commitPropertyScope() {
            graph.storage().put(join(iid, Schema.Property.SCOPE.infix().key()), scope.getBytes());
        }

        private void commitPropertyAbstract() {
            graph.storage().put(join(iid, Schema.Property.ABSTRACT.infix().key()));
        }

        private void commitPropertyLabel() {
            graph.storage().put(join(iid, Schema.Property.LABEL.infix().key()), label.getBytes());
        }

        private void commitPropertyValueClass() {
            graph.storage().put(join(iid, Schema.Property.VALUE_CLASS.infix().key()), valueClass.key());
        }

        private void commitPropertyRegex() {
            graph.storage().put(join(iid, Schema.Property.REGEX.infix().key()), regex.getBytes());
        }

        private void commitEdges() {
            outs.forEach(Edge::commit);
            ins.forEach(Edge::commit);
        }

    }

    public static class Persisted extends TypeVertex {

        public Persisted(TypeGraph graph, byte[] iid, String label, @Nullable String scope) {
            super(graph, Schema.Vertex.Type.of(iid[0]), iid, label, scope);
        }

        public Persisted(TypeGraph graph, byte[] iid) {
            super(graph, Schema.Vertex.Type.of(iid[0]), iid,
                  new String(graph.storage().get(join(iid, Schema.Property.LABEL.infix().key()))),
                  getScope(graph, iid));
        }

        @Nullable
        private static String getScope(TypeGraph graph, byte[] iid) {
            byte[] scopeBytes = graph.storage().get(join(iid, Schema.Property.SCOPE.infix().key()));
            if (scopeBytes != null) return new String(scopeBytes);
            else return null;
        }

        @Override
        protected TypeAdjacencyImpl newAdjacency(TypeVertex owner, AdjacencyImpl.Direction direction) {
            return new TypeAdjacencyImpl.Persisted(owner, direction);
        }

        @Override
        public Schema.Status status() {
            return Schema.Status.PERSISTED;
        }

        @Override
        public TypeVertex label(String label) {
            graph.update(this, this.label, scope, label, scope);
            graph.storage().put(join(iid, Schema.Property.LABEL.infix().key()), label.getBytes());
            graph.storage().delete(index(this.label, scope));
            graph.storage().put(index(label, scope), iid);
            this.label = label;
            return this;
        }

        @Override
        public TypeVertex scope(String scope) {
            graph.update(this, label, this.scope, label, scope);
            graph.storage().put(join(iid, Schema.Property.SCOPE.infix().key()), scope.getBytes());
            graph.storage().delete(index(label, this.scope));
            graph.storage().put(index(label, scope), iid);
            this.scope = scope;
            return this;
        }

        @Override
        public boolean isAbstract() {
            if (isAbstract != null) return isAbstract;
            byte[] flag = graph.storage().get(join(iid, Schema.Property.ABSTRACT.infix().key()));
            isAbstract = flag != null;
            return isAbstract;
        }

        @Override
        public TypeVertex isAbstract(boolean isAbstract) {
            if (isAbstract) graph.storage().put(join(iid, Schema.Property.ABSTRACT.infix().key()));
            else graph.storage().delete(join(iid, Schema.Property.ABSTRACT.infix().key()));
            this.isAbstract = isAbstract;
            return this;
        }

        @Override
        public Schema.ValueClass valueClass() {
            if (valueClass != null) return valueClass;
            byte[] val = graph.storage().get(join(iid, Schema.Property.VALUE_CLASS.infix().key()));
            if (val != null) valueClass = Schema.ValueClass.of(val[0]);
            return valueClass;
        }

        @Override
        public TypeVertex valueClass(Schema.ValueClass valueClass) {
            graph.storage().put(join(iid, Schema.Property.VALUE_CLASS.infix().key()), valueClass.key());
            this.valueClass = valueClass;
            return this;
        }

        @Override
        public String regex() {
            if (regex != null) return regex;
            byte[] val = graph.storage().get(join(iid, Schema.Property.REGEX.infix().key()));
            if (val != null) regex = new String(val);
            return regex;
        }

        @Override
        public TypeVertex regex(String regex) {
            graph.storage().put(join(iid, Schema.Property.REGEX.infix().key()), regex.getBytes());
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
            Iterator<byte[]> keys = graph.storage().iterate(iid, (iid, value) -> iid);
            while (keys.hasNext()) graph.storage().delete(keys.next());
        }

        private void commitEdges() {
            outs.forEach(Edge::commit);
            ins.forEach(Edge::commit);
        }

    }
}
