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
 */

package com.vaticle.typedb.core.graph.structure.impl;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.graph.TypeGraph;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.iid.IndexIID;
import com.vaticle.typedb.core.graph.iid.StructureIID;
import com.vaticle.typedb.core.graph.structure.RuleStructure;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.Conjunctable;
import com.vaticle.typeql.lang.pattern.Conjunction;
import com.vaticle.typeql.lang.pattern.Negation;
import com.vaticle.typeql.lang.pattern.Pattern;
import com.vaticle.typeql.lang.pattern.constraint.TypeConstraint;
import com.vaticle.typeql.lang.pattern.variable.BoundVariable;
import com.vaticle.typeql.lang.pattern.variable.ThingVariable;
import com.vaticle.typeql.lang.pattern.variable.Variable;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.vaticle.typedb.core.common.collection.ByteArray.encodeString;
import static com.vaticle.typedb.core.common.collection.ByteArray.join;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.Iterators.link;
import static com.vaticle.typedb.core.graph.common.Encoding.Property.LABEL;
import static com.vaticle.typedb.core.graph.common.Encoding.Property.THEN;
import static com.vaticle.typedb.core.graph.common.Encoding.Property.WHEN;

public abstract class RuleStructureImpl implements RuleStructure {

    final TypeGraph graph;
    final AtomicBoolean isDeleted;
    final Conjunction<? extends Pattern> when;
    final ThingVariable<?> then;
    StructureIID.Rule iid;
    String label;

    private boolean isModified;

    RuleStructureImpl(TypeGraph graph, StructureIID.Rule iid, String label,
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
    public ByteArray getBytes() {
        return iid.bytes();
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
        com.vaticle.typeql.lang.pattern.Conjunction<Conjunctable> whenNormalised = when().normalise().patterns().get(0);
        FunctionalIterator<BoundVariable> positiveVariables = iterate(whenNormalised.patterns()).filter(Conjunctable::isVariable)
                .map(Conjunctable::asVariable);
        FunctionalIterator<BoundVariable> negativeVariables = iterate(whenNormalised.patterns()).filter(Conjunctable::isNegation)
                .flatMap(p -> negationVariables(p.asNegation()));
        FunctionalIterator<Label> whenPositiveLabels = getTypeLabels(positiveVariables);
        FunctionalIterator<Label> whenNegativeLabels = getTypeLabels(negativeVariables);
        FunctionalIterator<Label> thenLabels = getTypeLabels(iterate(then().variables().iterator()));
        // filter out invalid labels as if they were truly invalid (eg. not relation:friend) we will catch it validation
        // this lets us index only types the user can actually retrieve as a concept
        return link(whenPositiveLabels, whenNegativeLabels, thenLabels)
                .filter(label -> graph.getType(label) != null).map(graph::getType);
    }

    private FunctionalIterator<BoundVariable> negationVariables(Negation<?> ruleNegation) {
        assert ruleNegation.patterns().size() == 1 && ruleNegation.patterns().get(0).isDisjunction();
        return iterate(ruleNegation.patterns().get(0).asDisjunction().patterns())
                .flatMap(pattern -> iterate(pattern.asConjunction().patterns())).map(Pattern::asVariable);
    }

    private FunctionalIterator<Label> getTypeLabels(FunctionalIterator<BoundVariable> variables) {
        return variables.flatMap(v -> iterate(connectedVars(v, new HashSet<>())))
                .distinct().filter(v -> v.isBound() && v.asBound().isType()).map(var -> var.asBound().asType().label()).filter(Optional::isPresent)
                .map(labelConstraint -> {
                    TypeConstraint.Label label = labelConstraint.get();
                    if (label.scope().isPresent()) return Label.of(label.label(), label.scope().get());
                    else return Label.of(label.label());
                });
    }

    private Set<Variable> connectedVars(Variable var, Set<Variable> visited) {
        visited.add(var);
        Set<Variable> vars = iterate(var.constraints()).flatMap(c -> iterate(c.variables())).map(v -> (Variable) v).toSet();
        if (visited.containsAll(vars)) return visited;
        else {
            visited.addAll(vars);
            return iterate(vars).flatMap(v -> iterate(connectedVars(v, visited))).toSet();
        }
    }

    public static class Buffered extends RuleStructureImpl {

        private final AtomicBoolean isCommitted;

        public Buffered(TypeGraph graph, StructureIID.Rule iid, String label, Conjunction<? extends Pattern> when, ThingVariable<?> then) {
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
        public Conjunction<? extends Pattern> when() { return when; }

        @Override
        public ThingVariable<?> then() { return then; }

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
            graph.storage().putUntracked(iid.bytes());
            graph.storage().putUntracked(IndexIID.Rule.of(label).bytes(), iid.bytes());
        }

        private void commitProperties() {
            commitPropertyLabel();
            commitWhen();
            commitThen();
        }

        private void commitPropertyLabel() {
            graph.storage().putUntracked(join(iid.bytes(), LABEL.infix().bytes()), encodeString(label));
        }

        private void commitWhen() {
            graph.storage().putUntracked(join(iid.bytes(), WHEN.infix().bytes()), encodeString(when().toString()));
        }

        private void commitThen() {
            graph.storage().putUntracked(join(iid.bytes(), THEN.infix().bytes()), encodeString(then().toString()));
        }

        private void indexReferences() {
            types().forEachRemaining(type -> graph.rules().references().buffered().put(this, type));
        }

    }

    public static class Persisted extends RuleStructureImpl {

        public Persisted(TypeGraph graph, StructureIID.Rule iid) {
            super(graph, iid,
                  graph.storage().get(join(iid.bytes(), LABEL.infix().bytes())).decodeString(),
                  TypeQL.parsePattern(graph.storage().get(join(iid.bytes(), WHEN.infix().bytes())).decodeString()).asConjunction(),
                  TypeQL.parseVariable(graph.storage().get(join(iid.bytes(), THEN.infix().bytes())).decodeString()).asThing()
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
        public ThingVariable<?> then() {
            return then;
        }

        @Override
        public void label(String label) {
            graph.rules().update(this, this.label, label);
            graph.storage().putUntracked(join(iid.bytes(), LABEL.infix().bytes()), encodeString(label));
            graph.storage().deleteUntracked(IndexIID.Rule.of(this.label).bytes());
            graph.storage().putUntracked(IndexIID.Rule.of(label).bytes(), iid.bytes());
            this.label = label;
        }

        @Override
        public void delete() {
            if (isDeleted.compareAndSet(false, true)) {
                graph.rules().references().delete(this, types());
                deleteVertexFromGraph();
                deleteVertexFromStorage();
            }
        }

        private void deleteVertexFromStorage() {
            graph.storage().deleteUntracked(IndexIID.Rule.of(label).bytes());
            FunctionalIterator<ByteArray> keys = graph.storage().iterate(iid.bytes(), (iid, value) -> iid);
            while (keys.hasNext()) graph.storage().deleteUntracked(keys.next());
        }

        @Override
        public void commit() {}
    }

}
