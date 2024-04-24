/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.graph.structure.impl;

import com.vaticle.typedb.core.common.collection.KeyValue;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.iid.IndexIID;
import com.vaticle.typedb.core.encoding.iid.PropertyIID;
import com.vaticle.typedb.core.encoding.iid.StructureIID;
import com.vaticle.typedb.core.encoding.key.Key;
import com.vaticle.typedb.core.graph.TypeGraph;
import com.vaticle.typedb.core.graph.structure.RuleStructure;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.common.TypeQLVariable;
import com.vaticle.typeql.lang.pattern.Conjunctable;
import com.vaticle.typeql.lang.pattern.Conjunction;
import com.vaticle.typeql.lang.pattern.Negation;
import com.vaticle.typeql.lang.pattern.Pattern;
import com.vaticle.typeql.lang.pattern.statement.Statement;
import com.vaticle.typeql.lang.pattern.statement.ThingStatement;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.vaticle.typedb.core.common.collection.ByteArray.encodeString;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.Iterators.link;
import static com.vaticle.typedb.core.encoding.Encoding.Property.Structure.LABEL;
import static com.vaticle.typedb.core.encoding.Encoding.Property.Structure.THEN;
import static com.vaticle.typedb.core.encoding.Encoding.Property.Structure.WHEN;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.STRING_ENCODING;

public abstract class RuleStructureImpl implements RuleStructure {

    final TypeGraph graph;
    final AtomicBoolean isDeleted;
    final Conjunction<? extends Pattern> when;
    final ThingStatement<?> then;
    StructureIID.Rule iid;
    String label;

    private boolean isModified;

    RuleStructureImpl(TypeGraph graph, StructureIID.Rule iid, String label,
                      Conjunction<? extends Pattern> when, ThingStatement<?> then) {
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

    @Override
    public void indexConcludesVertex(Label type) {
        graph.rules().conclusions().buffered().concludesVertex(this, graph.getType(type));
    }

    @Override
    public void unindexConcludesVertex(Label type) {
        graph.rules().conclusions().deleteConcludesVertex(this, graph.getType(type));
    }

    @Override
    public void indexConcludesEdgeTo(Label type) {
        graph.rules().conclusions().buffered().concludesEdgeTo(this, graph.getType(type));
    }

    @Override
    public void unindexConcludesEdgeTo(Label type) {
        graph.rules().conclusions().deleteConcludesEdgeTo(this, graph.getType(type));
    }

    public Encoding.Structure encoding() {
        return iid.encoding();
    }

    void deleteVertexFromGraph() {
        graph.rules().delete(this);
    }

    FunctionalIterator<TypeVertex> types() {
        return iterate(when().normalise().patterns()).flatMap(whenNormalised -> {
            FunctionalIterator<Statement> positiveStatements = iterate(whenNormalised.patterns()).filter(Conjunctable::isStatement)
                    .map(Conjunctable::asStatement);
            FunctionalIterator<Statement> negativeStatements = iterate(whenNormalised.patterns()).filter(Conjunctable::isNegation)
                    .flatMap(p -> negationVariables(p.asNegation()));
            FunctionalIterator<Label> whenPositiveLabels = getTypeLabels(positiveStatements);
            FunctionalIterator<Label> whenNegativeLabels = getTypeLabels(negativeStatements);
            FunctionalIterator<Label> thenLabels = getTypeLabels(iterate(then()));
            // filter out invalid labels as if they were truly invalid (eg. not relation:friend) we will catch it validation
            // this lets us index only types the user can actually retrieve as a concept
            return link(whenPositiveLabels, whenNegativeLabels, thenLabels)
                    .filter(label -> graph.getType(label) != null).map(graph::getType);
        });
    }

    private FunctionalIterator<Statement> negationVariables(Negation<?> ruleNegation) {
        assert ruleNegation.patterns().size() == 1 && ruleNegation.patterns().get(0).isDisjunction();
        return iterate(ruleNegation.patterns().get(0).asDisjunction().patterns())
                .flatMap(pattern -> iterate(pattern.asConjunction().patterns())).map(Pattern::asStatement);
    }

    private FunctionalIterator<Label> getTypeLabels(FunctionalIterator<Statement> statements) {
        return statements.flatMap(s -> iterate(s.variables().iterator()))
                .distinct().filter(TypeQLVariable::isLabelled).map(v -> {
                    if (v.reference().asLabel().scope().isPresent()) {
                        return Label.of(v.reference().asLabel().label(), v.reference().asLabel().scope().get());
                    } else {
                        return Label.of(v.reference().asLabel().label());
                    }
                });
    }

    public static class Buffered extends RuleStructureImpl {

        private final AtomicBoolean isCommitted;

        public Buffered(TypeGraph graph, StructureIID.Rule iid, String label, Conjunction<? extends Pattern> when, ThingStatement<?> then) {
            super(graph, iid, label, when, then);
            this.isCommitted = new AtomicBoolean(false);
            setModified();
            indexReferences();
        }

        @Override
        public void label(String label) {
            graph.rules().update(this, this.label, label);
            this.label = label;
        }

        @Override
        public Encoding.Status status() {
            return isCommitted.get() ? Encoding.Status.COMMITTED : Encoding.Status.BUFFERED;
        }

        @Override
        public Conjunction<? extends Pattern> when() {
            return when;
        }

        @Override
        public ThingStatement<?> then() {
            return then;
        }

        @Override
        public void delete() {
            if (isDeleted.compareAndSet(false, true)) {
                graph.rules().references().delete(this, types());
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
            graph.storage().putUntracked(iid);
            graph.storage().putUntracked(IndexIID.Rule.of(label), iid.bytes());
        }

        private void commitProperties() {
            commitPropertyLabel();
            commitWhen();
            commitThen();
        }

        private void commitPropertyLabel() {
            graph.storage().putUntracked(PropertyIID.Structure.of(iid, LABEL), encodeString(label, STRING_ENCODING));
        }

        private void commitWhen() {
            graph.storage().putUntracked(PropertyIID.Structure.of(iid, WHEN),
                    encodeString(when().toString(), STRING_ENCODING));
        }

        private void commitThen() {
            graph.storage().putUntracked(PropertyIID.Structure.of(iid, THEN),
                    encodeString(then().toString(), STRING_ENCODING));
        }

        private void indexReferences() {
            types().forEachRemaining(type -> graph.rules().references().buffered().put(this, type));
        }
    }

    public static class Persisted extends RuleStructureImpl {

        public Persisted(TypeGraph graph, StructureIID.Rule iid) {
            super(graph, iid,
                    graph.storage().get(PropertyIID.Structure.of(iid, LABEL)).decodeString(STRING_ENCODING),
                    TypeQL.parsePattern(graph.storage().get(PropertyIID.Structure.of(iid, WHEN))
                            .decodeString(STRING_ENCODING)).asConjunction(),
                    TypeQL.parseStatement(graph.storage().get(PropertyIID.Structure.of(iid, THEN))
                            .decodeString(STRING_ENCODING)).asThing()
            );
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
        public ThingStatement<?> then() {
            return then;
        }

        @Override
        public void label(String label) {
            graph.rules().update(this, this.label, label);
            graph.storage().putUntracked(PropertyIID.Structure.of(iid, LABEL), encodeString(label, STRING_ENCODING));
            graph.storage().deleteUntracked(IndexIID.Rule.of(this.label));
            graph.storage().putUntracked(IndexIID.Rule.of(label), iid.bytes());
            this.label = label;
        }

        @Override
        public void delete() {
            if (isDeleted.compareAndSet(false, true)) {
                graph.rules().references().delete(this, types());
                deleteVertexFromGraph();
                deleteVertexFromStorage();
                deletePropertiesFromStorage();
            }
        }

        private void deletePropertiesFromStorage() {
            Key.Prefix<PropertyIID.Structure> prefix = PropertyIID.Structure.prefix(iid);
            FunctionalIterator<PropertyIID.Structure> properties = graph.storage().iterate(prefix).map(KeyValue::key);
            while (properties.hasNext()) graph.storage().deleteUntracked(properties.next());
        }

        private void deleteVertexFromStorage() {
            graph.storage().deleteUntracked(IndexIID.Rule.of(label));
            graph.storage().deleteUntracked(iid);
        }

        @Override
        public void commit() {
        }
    }
}
