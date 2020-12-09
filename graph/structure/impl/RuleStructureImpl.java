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
 */

package grakn.core.graph.structure.impl;

import grakn.core.common.iterator.ResourceIterator;
import grakn.core.graph.SchemaGraph;
import grakn.core.graph.iid.IndexIID;
import grakn.core.graph.iid.StructureIID;
import grakn.core.graph.structure.RuleStructure;
import grakn.core.graph.util.Encoding;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.pattern.variable.ThingVariable;

import java.util.concurrent.atomic.AtomicBoolean;

import static grakn.core.common.collection.Bytes.join;
import static grakn.core.graph.util.Encoding.Property.LABEL;
import static grakn.core.graph.util.Encoding.Property.THEN;
import static grakn.core.graph.util.Encoding.Property.WHEN;

public abstract class RuleStructureImpl implements RuleStructure {

    final SchemaGraph graph;
    final AtomicBoolean isDeleted;
    final Conjunction<? extends Pattern> when;
    final ThingVariable<?> then;
    StructureIID.Rule iid;
    String label;

    private boolean isModified;

    RuleStructureImpl(SchemaGraph graph, StructureIID.Rule iid, String label,
                      Conjunction<? extends Pattern> when, ThingVariable<?> then) {
        assert when != null;
        assert then != null;
        this.graph = graph;
        this.iid = iid;
        this.label = label;
        this.when = when;
        this.then = then;
        this.isDeleted = new AtomicBoolean(false);
    }

    @Override
    public StructureIID.Rule iid() {
        return iid;
    }

    @Override
    public void iid(StructureIID.Rule iid) {
        this.iid = iid;
    }

    @Override
    public String label() {
        return label;
    }

    @Override
    public boolean isModified() {
        return isModified;
    }

    @Override
    public void setModified() {
        if (!isModified) {
            isModified = true;
            graph.setModified();
        }
    }

    @Override
    public boolean isDeleted() {
        return isDeleted.get();
    }

    public Encoding.Structure encoding() {
        return iid.encoding();
    }

    void deleteVertexFromGraph() {
        graph.delete(this);
    }

    public static class Buffered extends RuleStructureImpl {

        private final AtomicBoolean isCommitted;

        public Buffered(SchemaGraph graph, StructureIID.Rule iid, String label, Conjunction<? extends Pattern> when, ThingVariable<?> then) {
            super(graph, iid, label, when, then);
            this.isCommitted = new AtomicBoolean(false);
            setModified();
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
                deleteVertexFromGraph();
            }
        }

        @Override
        public void commit() {
            if (isCommitted.compareAndSet(false, true)) {
                commitVertex();
                commitProperties();
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

    public static class Persisted extends RuleStructureImpl {

        public Persisted(SchemaGraph graph, StructureIID.Rule iid) {
            super(graph, iid,
                  new String(graph.storage().get(join(iid.bytes(), LABEL.infix().bytes()))),
                  Graql.parsePattern(new String(graph.storage().get(join(iid.bytes(), WHEN.infix().bytes())))).asConjunction(),
                  Graql.parseVariable(new String(graph.storage().get(join(iid.bytes(), THEN.infix().bytes())))).asThing());
        }

        @Override
        public Encoding.Status status() {
            return Encoding.Status.PERSISTED;
        }

        @Override
        public Conjunction<? extends Pattern> when() {
            return when;
        }

        @Override
        public ThingVariable<?> then() {
            return then;
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
        public void commit() { }

        @Override
        public void delete() {
            if (isDeleted.compareAndSet(false, true)) {
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
