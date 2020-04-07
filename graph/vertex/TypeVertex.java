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

import hypergraph.common.iterator.Iterators;
import hypergraph.graph.Graph;
import hypergraph.graph.KeyGenerator;
import hypergraph.graph.Schema;
import hypergraph.graph.edge.Edge;
import hypergraph.graph.edge.TypeEdge;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;

import static hypergraph.common.collection.ByteArrays.join;

public abstract class TypeVertex extends Vertex<Schema.Vertex.Type, TypeVertex, Schema.Edge.Type, TypeEdge> {

    protected final Graph.Type graph;
    protected String label;
    protected String scope;
    protected Boolean isAbstract;
    protected Schema.DataType dataType;
    protected String regex;

    TypeVertex(Graph.Type graph, Schema.Vertex.Type type, byte[] iid, String label, @Nullable String scope) {
        super(iid, type);
        this.graph = graph;
        this.label = label;
        this.scope = scope;
    }

    public static String scopedLabel(String label, @Nullable String scope) {
        if (scope == null) return label;
        else return scope + ":" + label;
    }

    public static byte[] index(String label, @Nullable String scope) {
        return join(Schema.Index.TYPE.prefix().key(), scopedLabel(label, scope).getBytes());
    }

    public static byte[] generateIID(KeyGenerator keyGenerator, Schema.Vertex.Type schema) {
        return join(schema.prefix().key(), keyGenerator.forType(schema.root()));
    }

    public Graph.Type graph() {
        return graph;
    }

    public String label() {
        return label;
    }

    public String scopedLabel() {
        return scopedLabel(label, scope);
    }

    protected byte[] index() {
        return index(label, scope);
    }

    public abstract TypeVertex label(String label);

    public abstract TypeVertex scope(String scope);

    public abstract boolean isAbstract();

    public abstract TypeVertex setAbstract(boolean isAbstract);

    public abstract Schema.DataType dataType();

    public abstract TypeVertex dataType(Schema.DataType dataType);

    public abstract String regex();

    public abstract TypeVertex regex(String regex);

    public abstract class DirectedTypeEdges extends DirectedEdges<TypeVertex, Schema.Edge.Type, TypeEdge> {

        DirectedTypeEdges(Direction direction) {
            super(direction);
        }

        @Override
        public void put(Schema.Edge.Type schema, TypeVertex adjacent) {
            TypeVertex from = direction.isOut() ? TypeVertex.this : adjacent;
            TypeVertex to = direction.isOut() ? adjacent : TypeVertex.this;
            TypeEdge edge = new TypeEdge.Buffered(graph, schema, from, to);
            edges.computeIfAbsent(edge.schema(), e -> ConcurrentHashMap.newKeySet()).add(edge);
            to.ins().putNonRecursive(edge);
        }

        @Override
        public void deleteNonRecursive(TypeEdge edge) {
            if (edges.containsKey(edge.schema())) edges.get(edge.schema()).remove(edge);
        }

        @Override
        protected void deleteAll() {
            for (Schema.Edge.Type schema : Schema.Edge.Type.values()) delete(schema);
        }
    }

    public static class Buffered extends TypeVertex {

        public Buffered(Graph.Type graph, Schema.Vertex.Type schema, byte[] iid, String label, @Nullable String scope) {
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
        protected DirectedTypeEdges newDirectedEdges(DirectedEdges.Direction direction) {
            return new BufferedDirectedTypeEdges(direction);
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
            graph.storage().put(index(), iid);
        }

        void commitProperties() {
            commitPropertyLabel();
            if (scope != null) commitPropertyScope();
            if (isAbstract != null && isAbstract) commitPropertyAbstract();
            if (dataType != null) commitPropertyDataType();
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

        private void commitPropertyDataType() {
            graph.storage().put(join(iid, Schema.Property.DATATYPE.infix().key()), dataType.value());
        }

        private void commitPropertyRegex() {
            graph.storage().put(join(iid, Schema.Property.REGEX.infix().key()), regex.getBytes());
        }

        private void commitEdges() {
            outs.forEach(Edge::commit);
            ins.forEach(Edge::commit);
        }

        public class BufferedDirectedTypeEdges extends DirectedTypeEdges {

            BufferedDirectedTypeEdges(Direction direction) {
                super(direction);
            }

            @Override
            public Iterator<TypeVertex> get(Schema.Edge.Type schema) {
                Function<TypeEdge, TypeVertex> vertexFn = direction.isOut() ? Edge::to : Edge::from;
                if (edges.get(schema) != null) return Iterators.apply(edges.get(schema).iterator(), vertexFn);
                return Collections.emptyIterator();
            }

            @Override
            public void delete(Schema.Edge.Type schema, TypeVertex adjacent) {
                if (edges.containsKey(schema)) {
                    Predicate<TypeEdge> predicate = direction.isOut()
                            ? e -> e.to().equals(adjacent)
                            : e -> e.from().equals(adjacent);
                    edges.get(schema).parallelStream().filter(predicate).forEach(Edge::delete);
                }
            }

            @Override
            public void delete(Schema.Edge.Type schema) {
                if (edges.containsKey(schema)) edges.get(schema).parallelStream().forEach(Edge::delete);
            }
        }
    }

    public static class Persisted extends TypeVertex {


        public Persisted(Graph.Type graph, byte[] iid, String label, @Nullable String scope) {
            super(graph, Schema.Vertex.Type.of(iid[0]), iid, label, scope);
        }

        public Persisted(Graph.Type graph, byte[] iid) {
            super(graph, Schema.Vertex.Type.of(iid[0]), iid,
                  new String(graph.storage().get(join(iid, Schema.Property.LABEL.infix().key()))),
                  getScope(graph, iid));
        }

        @Nullable
        private static String getScope(Graph.Type graph, byte[] iid) {
            byte[] scopeBytes = graph.storage().get(join(iid, Schema.Property.SCOPE.infix().key()));
            if (scopeBytes != null) return new String(scopeBytes);
            else return null;
        }

        @Override
        protected DirectedEdges<TypeVertex, Schema.Edge.Type, TypeEdge> newDirectedEdges(DirectedEdges.Direction direction) {
            return new PersistedDirectedTypeEdges(direction);
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

        @Override
        public void commit() {
            commitEdges();
        }

        @Override
        public void delete() {
            ins.deleteAll();
            outs.deleteAll();
            graph.delete(this);
            graph.storage().delete(index());
            Iterator<byte[]> keys = graph.storage().iterate(iid, (iid, value) -> iid);
            while (keys.hasNext()) graph.storage().delete(keys.next());
        }

        private void commitEdges() {
            outs.forEach(Edge::commit);
            ins.forEach(Edge::commit);
        }

        public class PersistedDirectedTypeEdges extends DirectedTypeEdges {

            PersistedDirectedTypeEdges(Direction direction) {
                super(direction);
            }

            @Override
            public Iterator<TypeVertex> get(Schema.Edge.Type schema) {
                Schema.Infix edgeInfix = direction.isOut() ? schema.out() : schema.in();
                Function<TypeEdge, TypeVertex> vertexFn = direction.isOut() ? Edge::to : Edge::from;

                Iterator<TypeEdge> storageIterator = graph.storage().iterate(
                        join(iid, edgeInfix.key()),
                        (iid, value) -> new TypeEdge.Persisted(graph, iid)
                );

                if (edges.get(schema) != null) return Iterators.link(edges.get(schema).iterator(),
                                                                     storageIterator).apply(vertexFn);
                else return Iterators.apply(storageIterator, vertexFn);
            }

            @Override
            public void delete(Schema.Edge.Type schema, TypeVertex adjacent) {
                Optional<TypeEdge> container;
                Predicate<TypeEdge> predicate = direction.isOut()
                        ? e -> e.to().equals(adjacent)
                        : e -> e.from().equals(adjacent);

                if (edges.containsKey(schema) &&
                        (container = edges.get(schema).stream().filter(predicate).findAny()).isPresent()) {
                    edges.get(schema).remove(container.get());
                } else {
                    Schema.Infix infix = direction.isOut() ? schema.out() : schema.in();
                    byte[] edgeIID = join(iid, infix.key(), adjacent.iid);
                    if (graph.storage().get(edgeIID) != null) {
                        (new TypeEdge.Persisted(graph, edgeIID)).delete();
                    }
                }
            }

            @Override
            public void delete(Schema.Edge.Type schema) {
                if (edges.containsKey(schema)) edges.get(schema).parallelStream().forEach(Edge::delete);
                Iterator<TypeEdge> storageIterator = graph.storage().iterate(
                        join(iid, direction.isOut() ? schema.out().key() : schema.in().key()),
                        (iid, value) -> new TypeEdge.Persisted(graph, iid)
                );
                storageIterator.forEachRemaining(Edge::delete);
            }
        }
    }
}
