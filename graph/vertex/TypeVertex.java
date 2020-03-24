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

import hypergraph.graph.Graph;
import hypergraph.graph.KeyGenerator;
import hypergraph.graph.Schema;
import hypergraph.graph.edge.Edge;
import hypergraph.graph.edge.TypeEdge;
import hypergraph.graph.util.LinkedIterators;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import static hypergraph.graph.util.ByteArrays.join;

public abstract class TypeVertex extends Vertex<Schema.Vertex.Type, TypeVertex, Schema.Edge.Type, TypeEdge> {

    protected final Graph.Type graph;
    protected String label;
    protected Boolean isAbstract;
    protected Schema.DataType dataType;
    protected String regex;


    TypeVertex(Graph.Type graph, Schema.Vertex.Type type, byte[] iid, String label) {
        super(iid, type);
        this.graph = graph;
        this.label = label;
    }

    public static byte[] generateIID(KeyGenerator keyGenerator, Schema.Vertex.Type schema) {
        return join(schema.prefix().key(), keyGenerator.forType(schema.root()));
    }

    public static byte[] generateIndex(String label) {
        return join(Schema.Index.TYPE.prefix().key(), label.getBytes());
    }

    public String label() {
        return label;
    }

    @Override
    public TypeVertex out(Schema.Edge.Type schema, TypeVertex to) {
        TypeEdge edge = new TypeEdge.Buffered(graph, schema, this, to);
        outs.computeIfAbsent(edge.schema(), e -> ConcurrentHashMap.newKeySet()).add(edge);
        to.in(edge);
        return this;
    }

    @Override
    public TypeVertex in(Schema.Edge.Type schema, TypeVertex from) {
        TypeEdge edge = new TypeEdge.Buffered(graph, schema, from, this);
        ins.computeIfAbsent(edge.schema(), e -> ConcurrentHashMap.newKeySet()).add(edge);
        from.out(edge);
        return this;
    }

    public abstract boolean isAbstract();

    public abstract TypeVertex setAbstract(boolean isAbstract);

    public abstract Schema.DataType dataType();

    public abstract TypeVertex dataType(Schema.DataType dataType);

    public abstract String regex();

    public abstract TypeVertex regex(String regex);

    public static class Buffered extends TypeVertex {

        public Buffered(Graph.Type graph, Schema.Vertex.Type schema, byte[] iid, String label) {
            super(graph, schema, iid, label);
        }

        public Schema.Status status() {
            return Schema.Status.BUFFERED;
        }

        public boolean isAbstract() {
            return isAbstract;
        }

        public TypeVertex setAbstract(boolean isAbstract) {
            this.isAbstract = isAbstract;
            return this;
        }

        public Schema.DataType dataType() {
            return dataType;
        }

        public TypeVertex dataType(Schema.DataType dataType) {
            this.dataType = dataType;
            return this;
        }

        public String regex() {
            return regex;
        }

        public TypeVertex regex(String regex) {
            this.regex = regex;
            return this;
        }

        public Iterator<TypeEdge> outs(Schema.Edge.Type schema) {
            if (outs.get(schema) != null) return outs.get(schema).iterator();
            return null;
        }

        public Iterator<TypeEdge> ins(Schema.Edge.Type schema) {
            if (ins.get(schema) != null) return ins.get(schema).iterator();
            return null;
        }

        @Override
        public void commit() {
            graph.storage().put(iid);
            commitIndex();
            commitProperties();
            commitEdges();
        }

        void commitIndex() {
            graph.storage().put(join(Schema.Index.TYPE.prefix().key(), label.getBytes()), iid);
        }

        void commitProperties() {
            commitPropertyLabel();
            if (isAbstract != null && !isAbstract) commitPropertyAbstract();
            if (dataType != null) commitPropertyDataType();
            if (regex != null && !regex.isEmpty()) commitPropertyRegex();
        }

        void commitPropertyAbstract() {
            graph.storage().put(join(iid, Schema.Property.ABSTRACT.infix().key()));
        }

        void commitPropertyLabel() {
            graph.storage().put(join(iid, Schema.Property.LABEL.infix().key()), label.getBytes());
        }

        void commitPropertyDataType() {
            graph.storage().put(join(iid, Schema.Property.DATATYPE.infix().key()), dataType.value());
        }

        void commitPropertyRegex() {
            graph.storage().put(join(iid, Schema.Property.REGEX.infix().key()), regex.getBytes());
        }

        void commitEdges() {
            outs.forEach((key, set) -> set.forEach(Edge::commit));
            ins.forEach((key, set) -> set.forEach(Edge::commit));
        }
    }

    public static class Persisted extends TypeVertex {

        public Persisted(Graph.Type graph, byte[] iid, String label) {
            super(graph, Schema.Vertex.Type.of(iid[0]), iid, label);
        }

        public Persisted(Graph.Type graph, byte[] iid) {
            super(graph, Schema.Vertex.Type.of(iid[0]), iid,
                  new String(graph.storage().get(join(iid, Schema.Property.LABEL.infix().key()))));
        }

        @Override
        public Schema.Status status() {
            return Schema.Status.PERSISTED;
        }

        @Override
        public boolean isAbstract() {
            if (isAbstract != null) return isAbstract;
            byte[] abs = graph.storage().get(join(iid, Schema.Property.ABSTRACT.infix().key()));
            isAbstract = abs != null;
            return isAbstract;
        }

        @Override
        public TypeVertex setAbstract(boolean isAbstract) {
            return null; // TODO
        }

        @Override
        public Schema.DataType dataType() {
            if (dataType != null) return dataType;
            byte[] val = graph.storage().get(join(iid, Schema.Property.DATATYPE.infix().key()));
            if (val != null) dataType = Schema.DataType.of(val[0]);
            return dataType;
        }

        @Override
        public TypeVertex dataType(Schema.DataType dataType) {
            return null; // TODO
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
            return null; // TODO
        }

        public Iterator<TypeEdge> outs(Schema.Edge.Type schema) {
            Iterator<TypeEdge> persistedIterator = graph.storage().iterate(
                    join(iid, schema.out().key()),
                    (key, value) -> new TypeEdge.Persisted(graph, key)
            );

            if (outs.get(schema) != null) return new LinkedIterators<>(outs.get(schema).iterator(), persistedIterator);
            else return persistedIterator;
        }

        public Iterator<TypeEdge> ins(Schema.Edge.Type schema) {
            Iterator<TypeEdge> persistedIterator = graph.storage().iterate(
                    join(iid, schema.in().key()),
                    (key, value) -> new TypeEdge.Persisted(graph, key)
            );

            if (ins.get(schema) != null) return new LinkedIterators<>(ins.get(schema).iterator(), persistedIterator);
            else return persistedIterator;
        }

        @Override
        public void commit() {
            commitEdges();
        }

        private void commitEdges() {
            outs.forEach((key, set) -> set.forEach(Edge::commit));
            ins.forEach((key, set) -> set.forEach(Edge::commit));
        }
    }
}
