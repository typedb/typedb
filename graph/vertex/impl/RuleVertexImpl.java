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
 *
 */

package grakn.core.graph.vertex.impl;

import grakn.core.common.iterator.ResourceIterator;
import grakn.core.graph.SchemaGraph;
import grakn.core.graph.adjacency.SchemaAdjacency;
import grakn.core.graph.adjacency.impl.SchemaAdjacencyImpl;
import grakn.core.graph.iid.IndexIID;
import grakn.core.graph.iid.VertexIID;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.RuleVertex;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.pattern.variable.ThingVariable;

import java.util.concurrent.atomic.AtomicBoolean;

import static grakn.core.common.collection.Bytes.join;
import static grakn.core.graph.util.Encoding.Property.LABEL;
import static grakn.core.graph.util.Encoding.Property.THEN;
import static grakn.core.graph.util.Encoding.Property.WHEN;

public abstract class RuleVertexImpl extends SchemaVertexImpl<VertexIID.Rule, Encoding.Vertex.Rule> implements RuleVertex {

    protected Conjunction<? extends Pattern> when;
    protected ThingVariable<?> then;

    RuleVertexImpl(SchemaGraph graph, VertexIID.Rule iid, String label,
                   Conjunction<? extends Pattern> when, ThingVariable<?> then) {
        super(graph, iid, label);
        assert when != null;
        assert then != null;
        this.when = when;
        this.then = then;
    }

    RuleVertexImpl(SchemaGraph graph, VertexIID.Rule iid, String label) {
        super(graph, iid, label);
    }

    public Encoding.Vertex.Rule encoding() {
        return iid.encoding();
    }

    void deleteVertexFromGraph() {
        graph.delete(this);
    }

    @Override
    public void when(Conjunction<? extends Pattern> when) {
        this.when = when;
    }

    @Override
    public void then(ThingVariable<?> then) {
        this.then = then;
    }

    @Override
    public boolean isRule() { return true; }

    @Override
    public RuleVertex asRule() { return this; }

    public static class Buffered extends RuleVertexImpl {

        private final AtomicBoolean isCommitted;

        public Buffered(SchemaGraph graph, VertexIID.Rule iid, String label, Conjunction<? extends Pattern> when, ThingVariable<?> then) {
            super(graph, iid, label, when, then);
            this.isCommitted = new AtomicBoolean(false);
            setModified();
        }

        @Override
        protected SchemaAdjacency newAdjacency(Encoding.Direction direction) {
            return new SchemaAdjacencyImpl.Buffered(this, direction);
        }

        @Override
        public void label(String label) {
            graph.update(this, this.label, label);
            this.label = label;
        }

        @Override
        public Encoding.Status status() {
            return isCommitted.get() ? Encoding.Status.COMMITTED : Encoding.Status.BUFFERED;
        }

        @Override
        public Conjunction<? extends Pattern> when() { return when; }

        @Override
        public ThingVariable<?> then() { return then; }

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
            graph.storage().put(IndexIID.Rule.of(label).bytes(), iid.bytes());
        }

        private void commitProperties() {
            commitPropertyLabel();
            commitWhen();
            commitThen();
        }

        private void commitPropertyLabel() {
            graph.storage().put(join(iid.bytes(), LABEL.infix().bytes()), label.getBytes());
        }

        private void commitWhen() {
            graph.storage().put(join(iid.bytes(), WHEN.infix().bytes()), when().toString().getBytes());
        }

        private void commitThen() {
            graph.storage().put(join(iid.bytes(), THEN.infix().bytes()), then().toString().getBytes());
        }

    }

    public static class Persisted extends RuleVertexImpl {

        public Persisted(SchemaGraph graph, VertexIID.Rule iid) {
            super(graph, iid, new String(graph.storage().get(join(iid.bytes(), LABEL.infix().bytes()))));
        }

        @Override
        public Encoding.Status status() {
            return Encoding.Status.PERSISTED;
        }

        @Override
        public Conjunction<? extends Pattern> when() {
            if (when == null) {
                when = Graql.parsePattern(new String(graph.storage().get(join(iid.bytes(), WHEN.infix().bytes())))).asConjunction();
            }
            return when;
        }

        @Override
        public ThingVariable<?> then() {
            if (then == null) {
                then = Graql.parseVariable(new String(graph.storage().get(join(iid.bytes(), THEN.infix().bytes())))).asThing();
            }
            return then;
        }

        @Override
        protected SchemaAdjacency newAdjacency(Encoding.Direction direction) {
            return new SchemaAdjacencyImpl.Persisted(this, direction);
        }

        @Override
        public void label(String label) {
            graph.update(this, this.label, label);
            graph.storage().put(join(iid.bytes(), LABEL.infix().bytes()), label.getBytes());
            graph.storage().delete(IndexIID.Rule.of(this.label).bytes());
            graph.storage().put(IndexIID.Rule.of(label).bytes(), iid.bytes());
            this.label = label;
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
            graph.storage().delete(IndexIID.Rule.of(label).bytes());
            final ResourceIterator<byte[]> keys = graph.storage().iterate(iid.bytes(), (iid, value) -> iid);
            while (keys.hasNext()) graph.storage().delete(keys.next());
        }
    }

}
