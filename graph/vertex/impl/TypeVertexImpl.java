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

package grakn.graph.vertex.impl;

import grakn.common.exception.Error;
import grakn.common.exception.GraknException;
import grakn.graph.TypeGraph;
import grakn.graph.adjacency.Adjacency;
import grakn.graph.adjacency.TypeAdjacency;
import grakn.graph.adjacency.impl.TypeAdjacencyImpl;
import grakn.graph.edge.TypeEdge;
import grakn.graph.iid.EdgeIID;
import grakn.graph.iid.IndexIID;
import grakn.graph.iid.VertexIID;
import grakn.graph.util.Schema;
import grakn.graph.vertex.ThingVertex;
import grakn.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static grakn.common.collection.Bytes.join;
import static grakn.common.iterator.Iterators.distinct;
import static grakn.common.iterator.Iterators.link;

public abstract class TypeVertexImpl extends VertexImpl<VertexIID.Type> implements TypeVertex {

    protected final TypeGraph graph;
    protected final TypeAdjacency outs;
    protected final TypeAdjacency ins;
    protected final AtomicBoolean isDeleted;

    protected String label;
    protected String scope;
    protected Boolean isAbstract; // needs to be declared as the Boolean class
    protected Schema.ValueType valueType;
    protected String regex;


    TypeVertexImpl(TypeGraph graph, VertexIID.Type iid, String label, @Nullable String scope) {
        super(iid);
        this.graph = graph;
        this.label = label;
        this.scope = scope;
        this.outs = newAdjacency(Adjacency.Direction.OUT);
        this.ins = newAdjacency(Adjacency.Direction.IN);
        this.isDeleted = new AtomicBoolean(false);
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
    public void setModified() {
        if (!isModified) {
            isModified = true;
            graph.setModified();
        }
    }

    @Override
    public String label() {
        return label;
    }

    @Override
    public String scopedLabel() {
        return Schema.Vertex.Type.scopedLabel(label, scope);
    }

    @Override
    public boolean isDeleted() {
        return isDeleted.get();
    }

    void deleteEdges() {
        ins.deleteAll();
        outs.deleteAll();
    }

    void deleteVertexFromGraph() {
        graph.delete(this);
    }

    void commitEdges() {
        outs.forEach(TypeEdge::commit);
        ins.forEach(TypeEdge::commit);
    }

    public static class Buffered extends TypeVertexImpl {

        private final AtomicBoolean isCommitted;

        public Buffered(TypeGraph graph, VertexIID.Type iid, String label, @Nullable String scope) {
            super(graph, iid, label, scope);
            this.isCommitted = new AtomicBoolean(false);
            setModified();
        }

        @Override
        protected TypeAdjacency newAdjacency(Adjacency.Direction direction) {
            return new TypeAdjacencyImpl.Buffered(this, direction);
        }

        @Override
        public void buffer(ThingVertex thingVertex) {
            throw new GraknException(Error.Transaction.ILLEGAL_OPERATION);
        }

        @Override
        public void unbuffer(ThingVertex thingVertex) {
            throw new GraknException(Error.Transaction.ILLEGAL_OPERATION);
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
            return isCommitted.get() ? Schema.Status.COMMITTED : Schema.Status.BUFFERED;
        }

        @Override
        public boolean isAbstract() {
            return isAbstract != null ? isAbstract : false;
        }

        @Override
        public TypeVertexImpl isAbstract(boolean isAbstract) {
            this.isAbstract = isAbstract;
            this.setModified();
            return this;
        }

        @Override
        public Schema.ValueType valueType() {
            return valueType;
        }

        @Override
        public TypeVertexImpl valueType(Schema.ValueType valueType) {
            this.valueType = valueType;
            this.setModified();
            return this;
        }

        @Override
        public String regex() {
            return regex;
        }

        @Override
        public TypeVertexImpl regex(String regex) {
            this.regex = regex;
            this.setModified();
            return this;
        }

        @Override
        public void delete() {
            if (isDeleted.compareAndSet(false, true)) {
                deleteEdges();
                deleteVertexFromGraph();
            }
        }

        @Override
        public void commit() {
            if (isCommitted.compareAndSet(false, true)) {
                commitVertex();
                commitProperties();
                commitEdges();
            }
        }

        private void commitVertex() {
            graph.storage().put(iid.bytes());
            graph.storage().put(IndexIID.Type.of(label, scope).bytes(), iid.bytes());
        }

        private void commitProperties() {
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
    }

    public static class Persisted extends TypeVertexImpl {

        private final Set<ThingVertex> instances;
        private boolean regexLookedUp;

        public Persisted(TypeGraph graph, VertexIID.Type iid, String label, @Nullable String scope) {
            super(graph, iid, label, scope);
            instances = ConcurrentHashMap.newKeySet();
            regexLookedUp = false;
        }

        public Persisted(TypeGraph graph, VertexIID.Type iid) {
            super(graph, iid,
                  new String(graph.storage().get(join(iid.bytes(), Schema.Property.LABEL.infix().bytes()))),
                  getScope(graph, iid));
            instances = ConcurrentHashMap.newKeySet();
        }

        @Nullable
        private static String getScope(TypeGraph graph, VertexIID.Type iid) {
            byte[] scopeBytes = graph.storage().get(join(iid.bytes(), Schema.Property.SCOPE.infix().bytes()));
            if (scopeBytes != null) return new String(scopeBytes);
            else return null;
        }

        @Override
        protected TypeAdjacency newAdjacency(Adjacency.Direction direction) {
            return new TypeAdjacencyImpl.Persisted(this, direction);
        }

        @Override
        public void buffer(ThingVertex thingVertex) {
            instances.add(thingVertex);
        }

        @Override
        public void unbuffer(ThingVertex thingVertex) {
            instances.remove(thingVertex); // keep the cast to avoid warning
        }

        @Override
        public Iterator<ThingVertex> instances() {
            Iterator<ThingVertex> storageIterator = graph.storage().iterate(
                    join(iid.bytes(), Schema.Edge.ISA.in().bytes()),
                    (key, value) -> graph.thing().convert(EdgeIID.InwardsISA.of(key).end())
            );
            if (instances.isEmpty()) return storageIterator;
            else return distinct(link(instances.iterator(), storageIterator));
        }

        @Override
        public void iid(VertexIID.Type iid) {
            throw new GraknException(Error.Transaction.ILLEGAL_OPERATION);
        }

        @Override
        public Schema.Status status() {
            return Schema.Status.PERSISTED;
        }

        @Override
        public TypeVertexImpl label(String label) {
            graph.update(this, this.label, scope, label, scope);
            graph.storage().put(join(iid.bytes(), Schema.Property.LABEL.infix().bytes()), label.getBytes());
            graph.storage().delete(IndexIID.Type.of(this.label, scope).bytes());
            graph.storage().put(IndexIID.Type.of(label, scope).bytes(), iid.bytes());
            this.label = label;
            return this;
        }

        @Override
        public TypeVertexImpl scope(String scope) {
            graph.update(this, label, this.scope, label, scope);
            graph.storage().put(join(iid.bytes(), Schema.Property.SCOPE.infix().bytes()), scope.getBytes());
            graph.storage().delete(IndexIID.Type.of(label, this.scope).bytes());
            graph.storage().put(IndexIID.Type.of(label, scope).bytes(), iid.bytes());
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
            this.setModified();
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
            this.setModified();
            return this;
        }

        @Override
        public String regex() {
            if (regexLookedUp) return regex;
            regexLookedUp = true;
            byte[] val = graph.storage().get(join(iid.bytes(), Schema.Property.REGEX.infix().bytes()));
            if (val != null) regex = new String(val);
            return regex;
        }

        @Override
        public TypeVertexImpl regex(String regex) {
            graph.storage().put(join(iid.bytes(), Schema.Property.REGEX.infix().bytes()), regex.getBytes());
            this.regex = regex;
            this.setModified();
            return this;
        }

        @Override
        public void commit() {
            commitEdges();
        }

        @Override
        public void delete() {
            if (isDeleted.compareAndSet(false, true)) {
                deleteEdges();
                deleteVertexFromGraph();
                deleteVertexFromStorage();
            }
        }

        private void deleteVertexFromStorage() {
            graph.storage().delete(IndexIID.Type.of(label, scope).bytes());
            Iterator<byte[]> keys = graph.storage().iterate(iid.bytes(), (iid, value) -> iid);
            while (keys.hasNext()) graph.storage().delete(keys.next());
        }
    }
}
