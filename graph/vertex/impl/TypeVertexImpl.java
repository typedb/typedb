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

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.collection.KeyValue;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.graph.TypeGraph;
import com.vaticle.typedb.core.graph.adjacency.TypeAdjacency;
import com.vaticle.typedb.core.graph.adjacency.impl.TypeAdjacencyImpl;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.common.Storage.Key;
import com.vaticle.typedb.core.graph.iid.IndexIID;
import com.vaticle.typedb.core.graph.iid.PropertyIID;
import com.vaticle.typedb.core.graph.iid.VertexIID;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static com.vaticle.typedb.core.common.collection.ByteArray.encodeString;
import static com.vaticle.typedb.core.common.iterator.Iterators.link;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.OWNS;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.OWNS_KEY;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.PLAYS;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.RELATES;
import static com.vaticle.typedb.core.graph.common.Encoding.Property.ABSTRACT;
import static com.vaticle.typedb.core.graph.common.Encoding.Property.LABEL;
import static com.vaticle.typedb.core.graph.common.Encoding.Property.REGEX;
import static com.vaticle.typedb.core.graph.common.Encoding.Property.SCOPE;
import static com.vaticle.typedb.core.graph.common.Encoding.Property.VALUE_TYPE;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.STRING_ENCODING;
import static com.vaticle.typedb.core.graph.common.Encoding.Vertex.Type.ATTRIBUTE_TYPE;
import static com.vaticle.typedb.core.graph.common.Encoding.Vertex.Type.ENTITY_TYPE;
import static com.vaticle.typedb.core.graph.common.Encoding.Vertex.Type.RELATION_TYPE;
import static com.vaticle.typedb.core.graph.common.Encoding.Vertex.Type.ROLE_TYPE;
import static java.lang.Math.toIntExact;

public abstract class TypeVertexImpl extends VertexImpl<VertexIID.Type> implements TypeVertex {

    private static final int UNSET_COUNT = -1;

    final TypeGraph graph;
    final AtomicBoolean isDeleted;
    final TypeAdjacency.Out outs;
    final TypeAdjacency.In ins;
    boolean isModified;
    String label;
    String scope;
    Boolean isAbstract; // needs to be declared as the Boolean class
    Encoding.ValueType valueType;
    Pattern regex;

    private volatile int outOwnsCount;
    private volatile int outPlaysCount;
    private volatile int outRelatesCount;
    private volatile int inOwnsCount;
    private volatile int inPlaysCount;

    TypeVertexImpl(TypeGraph graph, VertexIID.Type iid, String label, @Nullable String scope) {
        super(iid);
        assert iid.isType();
        this.graph = graph;
        this.label = label;
        this.scope = scope;
        this.isDeleted = new AtomicBoolean(false);
        this.outs = newOutAdjacency();
        this.ins = newInAdjacency();
        outOwnsCount = UNSET_COUNT;
        outPlaysCount = UNSET_COUNT;
        outRelatesCount = UNSET_COUNT;
        inOwnsCount = UNSET_COUNT;
        inPlaysCount = UNSET_COUNT;
    }

    @Override
    public TypeGraph graph() {
        return graph;
    }

    @Override
    public void setModified() {
        if (!isModified) {
            isModified = true;
            graph.setModified();
        }
    }

    @Override
    public boolean isModified() {
        return isModified;
    }

    @Override
    public boolean isDeleted() {
        return isDeleted.get();
    }

    @Override
    public TypeAdjacency.Out outs() {
        return outs;
    }

    @Override
    public TypeAdjacency.In ins() {
        return ins;
    }

    @Override
    public Encoding.Vertex.Type encoding() {
        return iid.encoding();
    }

    @Override
    public boolean isType() {
        return true;
    }

    @Override
    public TypeVertex asType() {
        return this;
    }

    @Override
    public String label() {
        return label;
    }

    @Override
    public String scope() {
        return scope;
    }

    @Override
    public String scopedLabel() {
        return Encoding.Vertex.Type.scopedLabel(label, scope);
    }

    @Override
    public Label properLabel() {
        return Label.of(label, scope);
    }

    protected abstract TypeAdjacency.In newInAdjacency();

    protected abstract TypeAdjacency.Out newOutAdjacency();

    @Override
    public boolean isEntityType() {
        return encoding().equals(ENTITY_TYPE);
    }

    @Override
    public boolean isAttributeType() {
        return encoding().equals(ATTRIBUTE_TYPE);
    }

    @Override
    public boolean isRelationType() {
        return encoding().equals(RELATION_TYPE);
    }

    @Override
    public boolean isRoleType() {
        return encoding().equals(ROLE_TYPE);
    }

    @Override
    public int outOwnsCount(boolean isKey) {
        Supplier<Integer> function = () -> {
            if (isKey) return toIntExact(outs.edge(OWNS_KEY).to().stream().count());
            else return toIntExact(link(outs.edge(OWNS).to(), outs.edge(OWNS_KEY).to()).stream().count());
        };
        if (graph.isReadOnly()) {
            if (outOwnsCount == UNSET_COUNT) outOwnsCount = function.get();
            return outOwnsCount;
        } else {
            return function.get();
        }
    }

    @Override
    public int inOwnsCount(boolean isKey) {
        Supplier<Integer> function = () -> {
            if (isKey) return toIntExact(ins.edge(OWNS_KEY).from().stream().count());
            else return toIntExact(link(ins.edge(OWNS).from(), ins.edge(OWNS_KEY).from()).stream().count());
        };
        if (graph.isReadOnly()) {
            if (inOwnsCount == UNSET_COUNT) inOwnsCount = function.get();
            return inOwnsCount;
        } else {
            return function.get();
        }
    }

    @Override
    public int outPlaysCount() {
        Supplier<Integer> function = () -> toIntExact(outs.edge(PLAYS).to().stream().count());
        if (graph.isReadOnly()) {
            if (outPlaysCount == UNSET_COUNT) outPlaysCount = function.get();
            return outPlaysCount;
        } else {
            return function.get();
        }
    }

    @Override
    public int inPlaysCount() {
        Supplier<Integer> function = () -> toIntExact(ins.edge(PLAYS).from().stream().count());
        if (graph.isReadOnly()) {
            if (inPlaysCount == UNSET_COUNT) inPlaysCount = function.get();
            return inPlaysCount;
        } else {
            return function.get();
        }
    }

    @Override
    public int outRelatesCount() {
        Supplier<Integer> function = () -> toIntExact(outs.edge(RELATES).to().stream().count());
        if (graph.isReadOnly()) {
            if (outRelatesCount == UNSET_COUNT) outRelatesCount = function.get();
            return outRelatesCount;
        } else {
            return function.get();
        }
    }

    void deleteVertexFromGraph() {
        graph.delete(this);
    }

    void deleteEdges() {
        ins.deleteAll();
        outs.deleteAll();
    }

    void commitEdges() {
        outs.commit();
        ins.commit();
    }

    @Override
    public int compareTo(TypeVertex o) {
        return iid.compareTo(o.iid());
    }

    public static class Buffered extends TypeVertexImpl {

        private final AtomicBoolean isCommitted;

        public Buffered(TypeGraph graph, VertexIID.Type iid, String label, @Nullable String scope) {
            super(graph, iid, label, scope);
            this.isCommitted = new AtomicBoolean(false);
            setModified();
        }

        @Override
        protected TypeAdjacency.In newInAdjacency() {
            return new TypeAdjacencyImpl.Buffered.In(this);
        }

        @Override
        protected TypeAdjacency.Out newOutAdjacency() {
            return new TypeAdjacencyImpl.Buffered.Out(this);
        }

        @Override
        public void label(String label) {
            assert !isDeleted();
            graph.update(this, this.label, scope, label, scope);
            this.label = label;
        }

        @Override
        public void scope(String scope) {
            assert !isDeleted();
            graph.update(this, label, this.scope, label, scope);
            this.scope = scope;
        }

        @Override
        public Encoding.Status status() {
            return isCommitted.get() ? Encoding.Status.COMMITTED : Encoding.Status.BUFFERED;
        }

        @Override
        public boolean isAbstract() {
            return isAbstract != null ? isAbstract : false;
        }

        @Override
        public TypeVertexImpl isAbstract(boolean isAbstract) {
            assert !isDeleted();
            this.isAbstract = isAbstract;
            this.setModified();
            return this;
        }

        @Override
        public Encoding.ValueType valueType() {
            return valueType;
        }

        @Override
        public TypeVertexImpl valueType(Encoding.ValueType valueType) {
            assert !isDeleted();
            this.valueType = valueType;
            this.setModified();
            return this;
        }

        @Override
        public Pattern regex() {
            return regex;
        }

        @Override
        public TypeVertexImpl regex(Pattern regex) {
            assert !isDeleted();
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
            graph.storage().putUntracked(iid);
            graph.storage().putUntracked(IndexIID.Type.Label.of(label, scope), iid.bytes());
        }

        private void commitProperties() {
            commitPropertyLabel();
            if (scope != null) commitPropertyScope();
            if (isAbstract != null && isAbstract) commitPropertyAbstract();
            if (valueType != null) commitPropertyValueType();
            if (regex != null) commitPropertyRegex();
        }

        private void commitPropertyScope() {
            graph.storage().putUntracked(PropertyIID.Type.of(iid, SCOPE), encodeString(scope, STRING_ENCODING));
        }

        private void commitPropertyAbstract() {
            graph.storage().putUntracked(PropertyIID.Type.of(iid, ABSTRACT));
        }

        private void commitPropertyLabel() {
            graph.storage().putUntracked(PropertyIID.Type.of(iid, LABEL), encodeString(label, STRING_ENCODING));
        }

        private void commitPropertyValueType() {
            graph.storage().putUntracked(PropertyIID.Type.of(iid, VALUE_TYPE), valueType.bytes());
        }

        private void commitPropertyRegex() {
            graph.storage().putUntracked(PropertyIID.Type.of(iid, REGEX), encodeString(regex.pattern(), STRING_ENCODING));
        }
    }

    public static class Persisted extends TypeVertexImpl {

        private boolean regexLookedUp;

        public Persisted(TypeGraph graph, VertexIID.Type iid, String label, @Nullable String scope) {
            super(graph, iid, label, scope);
            regexLookedUp = false;
        }

        public Persisted(TypeGraph graph, VertexIID.Type iid) {
            super(graph, iid,
                    graph.storage().get(PropertyIID.Type.of(iid, LABEL)).decodeString(STRING_ENCODING),
                    getScope(graph, iid));
        }

        @Nullable
        private static String getScope(TypeGraph graph, VertexIID.Type iid) {
            ByteArray scopeBytes = graph.storage().get(PropertyIID.Type.of(iid, SCOPE));
            if (scopeBytes != null) return scopeBytes.decodeString(STRING_ENCODING);
            else return null;
        }

        @Override
        protected TypeAdjacency.In newInAdjacency(){
            return new TypeAdjacencyImpl.Persisted.In(this);
        }

        @Override
        protected TypeAdjacency.Out newOutAdjacency(){
            return new TypeAdjacencyImpl.Persisted.Out(this);
        }

        @Override
        public Encoding.Status status() {
            return Encoding.Status.PERSISTED;
        }

        @Override
        public void label(String label) {
            assert !isDeleted();
            graph.update(this, this.label, scope, label, scope);
            graph.storage().putUntracked(PropertyIID.Type.of(iid, LABEL), encodeString(label, STRING_ENCODING));
            graph.storage().deleteUntracked(IndexIID.Type.Label.of(this.label, scope));
            graph.storage().putUntracked(IndexIID.Type.Label.of(label, scope), iid.bytes());
            this.label = label;
        }

        @Override
        public void scope(String scope) {
            assert !isDeleted();
            graph.update(this, label, this.scope, label, scope);
            graph.storage().putUntracked(PropertyIID.Type.of(iid, SCOPE), encodeString(scope, STRING_ENCODING));
            graph.storage().deleteUntracked(IndexIID.Type.Label.of(label, this.scope));
            graph.storage().putUntracked(IndexIID.Type.Label.of(label, scope), iid.bytes());
            this.scope = scope;
        }

        @Override
        public boolean isAbstract() {
            if (isAbstract != null) return isAbstract;
            ByteArray flag = graph.storage().get(PropertyIID.Type.of(iid, ABSTRACT));
            isAbstract = flag != null;
            return isAbstract;
        }

        @Override
        public TypeVertexImpl isAbstract(boolean isAbstract) {
            assert !isDeleted();
            if (isAbstract) graph.storage().putUntracked(PropertyIID.Type.of(iid, ABSTRACT));
            else graph.storage().deleteUntracked(PropertyIID.Type.of(iid, ABSTRACT));
            this.isAbstract = isAbstract;
            this.setModified();
            return this;
        }

        @Override
        public Encoding.ValueType valueType() {
            if (valueType != null) return valueType;
            ByteArray val = graph.storage().get(PropertyIID.Type.of(iid, VALUE_TYPE));
            if (val != null) valueType = Encoding.ValueType.of(val.get(0));
            return valueType;
        }

        @Override
        public TypeVertexImpl valueType(Encoding.ValueType valueType) {
            assert !isDeleted();
            graph.storage().putUntracked(PropertyIID.Type.of(iid, VALUE_TYPE), valueType.bytes());
            this.valueType = valueType;
            this.setModified();
            return this;
        }

        @Override
        public Pattern regex() {
            if (regexLookedUp) return regex;
            ByteArray val = graph.storage().get(PropertyIID.Type.of(iid, REGEX));
            if (val != null) regex = Pattern.compile(val.decodeString(STRING_ENCODING));
            regexLookedUp = true;
            return regex;
        }

        @Override
        public TypeVertexImpl regex(Pattern regex) {
            assert !isDeleted();
            if (regex == null) graph.storage().deleteUntracked(PropertyIID.Type.of(iid, REGEX));
            else {
                graph.storage().putUntracked(PropertyIID.Type.of(iid, REGEX),
                        encodeString(regex.pattern(), STRING_ENCODING));
            }
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
                deletePropertiesFromStorage();
            }
        }

        private void deletePropertiesFromStorage() {
            Key.Prefix<PropertyIID.Type> prefix = PropertyIID.Type.prefix(iid);
            FunctionalIterator<PropertyIID.Type> properties = graph.storage().iterate(prefix).map(KeyValue::key);
            while (properties.hasNext()) graph.storage().deleteUntracked(properties.next());
        }

        private void deleteVertexFromStorage() {
            graph.storage().deleteUntracked(IndexIID.Type.Label.of(label, scope));
            graph.storage().deleteUntracked(iid);
        }
    }
}
