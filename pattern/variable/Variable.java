/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.pattern.variable;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.pattern.Pattern;
import com.vaticle.typedb.core.pattern.constraint.Constraint;
import com.vaticle.typedb.core.traversal.GraphTraversal;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.common.Reference;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public abstract class Variable implements Pattern {

    private final Set<Label> inferredTypes;
    private final int hash;
    final Identifier.Variable identifier;

    Variable(Identifier.Variable identifier) {
        this.identifier = identifier;
        this.hash = Objects.hash(identifier);
        this.inferredTypes = new HashSet<>();
    }

    public abstract Set<? extends Constraint> constraints();

    public abstract Set<Constraint> constraining();

    public abstract void constraining(Constraint constraint);

    public abstract Identifier.Variable id();

    public Reference reference() {
        return identifier.reference();
    }

    public abstract void addTo(GraphTraversal.Thing traversal);

    public boolean isType() {
        return false;
    }

    public boolean isThing() {
        return false;
    }

    public boolean isValue() {
        return false;
    }

    public TypeVariable asType() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(TypeVariable.class));
    }

    public ThingVariable asThing() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(ThingVariable.class));
    }

    public ValueVariable asValue() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(ValueVariable.class));
    }

    public void addInferredTypes(Label label) {
        inferredTypes.add(label);
    }

    public void addInferredTypes(Set<Label> labels) {
        inferredTypes.addAll(labels);
    }

    public void setInferredTypes(Set<Label> labels) {
        inferredTypes.clear();
        inferredTypes.addAll(labels);
    }

    public void retainInferredTypes(Set<Label> labels) {
        inferredTypes.retainAll(labels);
    }

    public Set<Label> inferredTypes() {
        return inferredTypes;
    }

    @Override
    public String toString() {
        if (identifier.isLabel()) return asType().label().get().properLabel().name();
        else return identifier.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Variable that = (Variable) o;
        return this.identifier.equals(that.identifier);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
