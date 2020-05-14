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
import hypergraph.graph.edge.EdgeMap;
import hypergraph.graph.edge.impl.EdgeMapImpl;
import hypergraph.graph.util.KeyGenerator;
import hypergraph.graph.util.Schema;
import hypergraph.graph.TypeGraph;
import hypergraph.graph.edge.Edge;
import hypergraph.graph.edge.TypeEdge;
import hypergraph.graph.edge.impl.TypeEdgeImpl;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import static hypergraph.common.collection.ByteArrays.join;
import static hypergraph.common.iterator.Iterators.link;

public abstract class TypeVertex extends Vertex<Schema.Vertex.Type, TypeVertex, Schema.Edge.Type, TypeEdge> {

    protected final TypeGraph graph;
    protected final TypeEdgeMap outs;
    protected final TypeEdgeMap ins;

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
        this.outs = newEdgeMap(this, EdgeMap.Direction.OUT);
        this.ins = newEdgeMap(this, EdgeMap.Direction.IN);
    }

    protected abstract TypeEdgeMap newEdgeMap(TypeVertex owner, EdgeMapImpl.Direction direction);

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

    public TypeEdgeMap outs() {
        return outs;
    }

    public TypeEdgeMap ins() {
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

    public static abstract class TypeEdgeMap extends EdgeMapImpl<Schema.Edge.Type, TypeEdge, TypeVertex> {

        TypeEdgeMap(TypeVertex owner, Direction direction) {
            super(owner, direction);
        }

        @Override
        public abstract TypeIteratorBuilder edge(Schema.Edge.Type schema);

        @Override
        public void put(Schema.Edge.Type schema, TypeVertex adjacent) {
            TypeVertex from = direction.isOut() ? owner : adjacent;
            TypeVertex to = direction.isOut() ? adjacent : owner;
            TypeEdge edge = new TypeEdgeImpl.Buffered(owner.graph(), schema, from, to);
            edges.computeIfAbsent(edge.schema(), e -> ConcurrentHashMap.newKeySet()).add(edge);
            to.ins().putNonRecursive(edge);
        }

        @Override
        public void deleteNonRecursive(TypeEdge edge) {
            if (edges.containsKey(edge.schema())) edges.get(edge.schema()).remove(edge);
        }

        @Override
        public void deleteAll() {
            for (Schema.Edge.Type schema : Schema.Edge.Type.values()) delete(schema);
        }

        public static class TypeIteratorBuilder extends IteratorBuilderImpl<TypeVertex, TypeEdge> {

            TypeIteratorBuilder(Iterator<TypeEdge> edgeIterator) {
                super(edgeIterator);
            }

            public Iterator<TypeVertex> overridden() {
                return Iterators.apply(edgeIterator, TypeEdge::overridden);
            }
        }
    }

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
        protected TypeEdgeMap newEdgeMap(TypeVertex owner, EdgeMapImpl.Direction direction) {
            return new BufferedTypeEdgeMap(owner, direction);
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

        public static class BufferedTypeEdgeMap extends TypeEdgeMap {

            BufferedTypeEdgeMap(TypeVertex owner, Direction direction) {
                super(owner, direction);
            }

            @Override
            public TypeIteratorBuilder edge(Schema.Edge.Type schema) {
                Set<TypeEdge> t;
                if ((t = edges.get(schema)) != null) return new TypeIteratorBuilder(t.iterator());
                return new TypeIteratorBuilder(Collections.emptyIterator());
            }

            @Override
            public TypeEdge edge(Schema.Edge.Type schema, TypeVertex adjacent) {
                if (edges.containsKey(schema)) {
                    Predicate<TypeEdge> predicate = direction.isOut()
                            ? e -> e.to().equals(adjacent)
                            : e -> e.from().equals(adjacent);
                    return edges.get(schema).stream().filter(predicate).findAny().orElse(null);
                }
                return null;
            }

            @Override
            public void delete(Schema.Edge.Type schema, TypeVertex adjacent) {
                if (edges.containsKey(schema)) {
                    Predicate<TypeEdge> predicate = direction.isOut()
                            ? e -> e.to().equals(adjacent)
                            : e -> e.from().equals(adjacent);
                    edges.get(schema).stream().filter(predicate).forEach(Edge::delete);
                }
            }

            @Override
            public void delete(Schema.Edge.Type schema) {
                if (edges.containsKey(schema)) edges.get(schema).forEach(Edge::delete);
            }
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
        protected TypeEdgeMap newEdgeMap(TypeVertex owner, EdgeMapImpl.Direction direction) {
            return new PersistedTypeEdgeMap(owner, direction);
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

        public class PersistedTypeEdgeMap extends TypeEdgeMap {

            PersistedTypeEdgeMap(TypeVertex owner, Direction direction) {
                super(owner, direction);
            }

            @Override
            public TypeIteratorBuilder edge(Schema.Edge.Type schema) {
                Iterator<TypeEdge> storageIterator = graph.storage().iterate(
                        join(iid, direction.isOut() ? schema.out().key() : schema.in().key()),
                        (key, value) -> new TypeEdgeImpl.Persisted(graph, key, value)
                );

                if (edges.get(schema) == null) {
                    return new TypeIteratorBuilder(storageIterator);
                } else {
                    return new TypeIteratorBuilder(link(edges.get(schema).iterator(), storageIterator));
                }
            }

            @Override
            public TypeEdge edge(Schema.Edge.Type schema, TypeVertex adjacent) {
                Optional<TypeEdge> container;
                Predicate<TypeEdge> predicate = direction.isOut()
                        ? e -> e.to().equals(adjacent)
                        : e -> e.from().equals(adjacent);

                if (edges.containsKey(schema) &&
                        (container = edges.get(schema).stream().filter(predicate).findAny()).isPresent()) {
                    return container.get();
                } else {
                    Schema.Infix infix = direction.isOut() ? schema.out() : schema.in();
                    byte[] edgeIID = join(iid, infix.key(), adjacent.iid);
                    byte[] overriddenIID;
                    if ((overriddenIID = graph.storage().get(edgeIID)) != null) {
                        return new TypeEdgeImpl.Persisted(graph, edgeIID, overriddenIID);
                    }
                }

                return null;
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
                    byte[] overriddenIID;
                    if ((overriddenIID = graph.storage().get(edgeIID)) != null) {
                        (new TypeEdgeImpl.Persisted(graph, edgeIID, overriddenIID)).delete();
                    }
                }
            }

            @Override
            public void delete(Schema.Edge.Type schema) {
                if (edges.containsKey(schema)) edges.get(schema).parallelStream().forEach(Edge::delete);
                Iterator<TypeEdge> storageIterator = graph.storage().iterate(
                        join(iid, direction.isOut() ? schema.out().key() : schema.in().key()),
                        (key, value) -> new TypeEdgeImpl.Persisted(graph, key, value)
                );
                storageIterator.forEachRemaining(Edge::delete);
            }
        }
    }
}
