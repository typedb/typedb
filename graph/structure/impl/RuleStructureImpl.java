/*
 * Copyright (C) 2021 Grakn Labs
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
import grakn.core.common.parameters.Label;
import grakn.core.graph.SchemaGraph;
import grakn.core.graph.iid.IndexIID;
import grakn.core.graph.iid.StructureIID;
import grakn.core.graph.structure.RuleStructure;
import grakn.core.graph.util.Encoding;
import graql.lang.Graql;
import graql.lang.pattern.Conjunctable;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Negation;
import graql.lang.pattern.Pattern;
import graql.lang.pattern.constraint.TypeConstraint;
import graql.lang.pattern.variable.BoundVariable;
import graql.lang.pattern.variable.ThingVariable;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.collection.Bytes.join;
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.common.iterator.Iterators.link;
import static grakn.core.graph.util.Encoding.Property.LABEL;
import static grakn.core.graph.util.Encoding.Property.THEN;
import static grakn.core.graph.util.Encoding.Property.WHEN;

public abstract class RuleStructureImpl implements RuleStructure {

    final SchemaGraph graph;
    final AtomicBoolean isDeleted;
    final AtomicBoolean isOutdated;
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
        this.isOutdated = new AtomicBoolean(false);
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
    public boolean isOutdated() {
        return isOutdated.get();
    }

    @Override
    public void isOutdated(boolean isOutdated) {
        this.isOutdated.set(isOutdated);
    }

    @Override
    public void createConcludesIndex(Label type) {
        graph.ruleIndex().ruleConcludes(this, type);
    }

    @Override
    public void clearConcludesIndex(Label type) {
        graph.ruleIndex().deleteRuleConcludes(this, type);
    }

    @Override
    public void createConcludesHasAttributeIndex(Label type) {
        graph.ruleIndex().ruleConcludesHasAttribute(this, type);
    }

    @Override
    public void clearConcludesHasAttributeIndex(Label type) {
        graph.ruleIndex().deleteRuleConcludesHasAttribute(this, type);
    }

    public Encoding.Structure encoding() {
        return iid.encoding();
    }

    void deleteVertexFromGraph() {
        graph.delete(this);
    }

    ResourceIterator<Label> validRuleLabels() {
        graql.lang.pattern.Conjunction<Conjunctable> whenNormalised = when().normalise().patterns().get(0);
        ResourceIterator<BoundVariable> positiveVariables = iterate(whenNormalised.patterns()).filter(Conjunctable::isVariable)
                .map(Conjunctable::asVariable);
        ResourceIterator<BoundVariable> negativeVariables = iterate(whenNormalised.patterns()).filter(Conjunctable::isNegation)
                .flatMap(p -> negationVariables(p.asNegation()));
        ResourceIterator<Label> whenPositiveLabels = getTypeLabels(positiveVariables);
        ResourceIterator<Label> whenNegativeLabels = getTypeLabels(negativeVariables);
        ResourceIterator<Label> thenLabels = getTypeLabels(iterate(then().variables().iterator()));
        // filter out invalid labels as if they were truly invalid (eg. not relation:friend) we will catch it validation
        // this lets us index only types the user can actually retrieve as a concept
        return link(whenPositiveLabels, whenNegativeLabels, thenLabels)
                .filter(label -> graph.getType(label) != null);
    }

    private ResourceIterator<BoundVariable> negationVariables(Negation<?> ruleNegation) {
        assert ruleNegation.patterns().size() == 1 && ruleNegation.patterns().get(0).isDisjunction();
        return iterate(ruleNegation.patterns().get(0).asDisjunction().patterns())
                .flatMap(pattern -> iterate(pattern.asConjunction().patterns())).map(Pattern::asVariable);
    }

    private ResourceIterator<Label> getTypeLabels(ResourceIterator<BoundVariable> variables) {
        return variables.filter(BoundVariable::isType).map(variable -> variable.asType().label())
                .filter(Optional::isPresent).map(labelConstraint -> {
                    TypeConstraint.Label label = labelConstraint.get();
                    if (label.scope().isPresent()) return Label.of(label.label(), label.scope().get());
                    else return Label.of(label.label());
                });
    }

    public static class Buffered extends RuleStructureImpl {

        private final AtomicBoolean isCommitted;

        public Buffered(SchemaGraph graph, StructureIID.Rule iid, String label, Conjunction<? extends Pattern> when, ThingVariable<?> then) {
            super(graph, iid, label, when, then);
            this.isCommitted = new AtomicBoolean(false);
            setModified();
            indexLabels();
        }

        private void indexLabels() {
            ResourceIterator<Label> labels = validRuleLabels();
            labels.forEachRemaining(label -> graph.ruleIndex().ruleContains(this, label));
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
                graph.ruleIndex().deleteBufferedRuleContains(this, validRuleLabels());
                deleteVertexFromGraph();
            }
        }

        @Override
        public void commit() {
            assert !isOutdated();
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
        public void commit() {
            assert !isOutdated();
        }

        @Override
        public void delete() {
            if (isDeleted.compareAndSet(false, true)) {
                graph.ruleIndex().deleteRuleContains(this, validRuleLabels());
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
