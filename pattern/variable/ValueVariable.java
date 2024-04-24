/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.pattern.variable;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.pattern.constraint.Constraint;
import com.vaticle.typedb.core.pattern.constraint.common.Predicate;
import com.vaticle.typedb.core.pattern.constraint.value.PredicateConstraint;
import com.vaticle.typedb.core.pattern.constraint.value.ValueConstraint;
import com.vaticle.typedb.core.pattern.constraint.value.AssignmentConstraint;
import com.vaticle.typedb.core.pattern.equivalence.AlphaEquivalence;
import com.vaticle.typedb.core.pattern.equivalence.AlphaEquivalent;
import com.vaticle.typedb.core.traversal.GraphTraversal;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.pattern.expression.Expression;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_OPERATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.VALUE_VARIABLE_DUPLICATE_ASSIGMENT;
import static java.util.Collections.emptySet;

public class ValueVariable extends Variable implements AlphaEquivalent<ValueVariable> {

    private AssignmentConstraint assigment;
    private final Set<Constraint> constraining;
    private final Set<ValueConstraint> constraints;

    public ValueVariable(Identifier.Variable identifier) {
        super(identifier);
        this.constraining = new HashSet<>();
        this.constraints = new HashSet<>();
        this.assigment = null;
    }

    public ValueVariable constrainValue(List<com.vaticle.typeql.lang.pattern.constraint.ValueConstraint> constraints, VariableRegistry registry) {
        constraints.forEach(constraint -> this.constrain(ValueConstraint.of(this, constraint, registry)));
        return this;
    }

    private void constrain(ValueConstraint valueConstraint) {
        if (valueConstraint.isAssignment()) {
            if (this.assigment != null) throw TypeDBException.of(VALUE_VARIABLE_DUPLICATE_ASSIGMENT, identifier);
            this.assigment = valueConstraint.asAssignment();
        }
        this.constraints.add(valueConstraint);
    }

    public ValueVariable clone() {
        return new ValueVariable(id());
    }

    void constrainClone(ValueVariable toClone, VariableCloner cloner) {
        toClone.constraints().forEach(constraint -> this.constrain(ValueConstraint.of(this, constraint, cloner)));
    }

    public AssignmentConstraint assign(Expression.Constant<?> constant) {
        AssignmentConstraint constraint = new AssignmentConstraint(this, constant, emptySet(), emptySet());
        constrain(constraint);
        return constraint;
    }

    public AssignmentConstraint assign(Expression typeQLExpression, Set<ThingVariable> thingArgs, Set<ValueVariable> valueArgs) {
        AssignmentConstraint constraint = new AssignmentConstraint(this, typeQLExpression, thingArgs, valueArgs);
        constrain(constraint);
        return constraint;
    }

    public AssignmentConstraint assignment() {
        return assigment;
    }

    public PredicateConstraint predicate(Predicate<?> predicate) {
        PredicateConstraint predicateConstraint = new PredicateConstraint(this, predicate);
        constrain(predicateConstraint);
        return predicateConstraint;
    }

    public Encoding.ValueType<?> valueType() {
        return (assigment != null) ? assigment.valueType() : null;
    }

    @Override
    public Set<ValueConstraint> constraints() {
        return constraints;
    }

    @Override
    public Set<Constraint> constraining() {
        return constraining;
    }

    @Override
    public void constraining(Constraint constraint) {
        this.constraining.add(constraint);
    }

    @Override
    public Identifier.Variable.Name id() {
        return identifier.asName();
    }

    @Override
    public void addTo(GraphTraversal.Thing traversal) {
        this.constraints.forEach(c -> c.addTo(traversal));
    }

    @Override
    public void addInferredTypes(Label label) {
        throw TypeDBException.of(ILLEGAL_OPERATION);
    }

    @Override
    public void addInferredTypes(Set<Label> labels) {
        throw TypeDBException.of(ILLEGAL_OPERATION);
    }

    @Override
    public void setInferredTypes(Set<Label> labels) {
        throw TypeDBException.of(ILLEGAL_OPERATION);
    }

    @Override
    public void retainInferredTypes(Set<Label> labels) {
        throw TypeDBException.of(ILLEGAL_OPERATION);
    }

    @Override
    public Set<Label> inferredTypes() {
        throw TypeDBException.of(ILLEGAL_OPERATION);
    }

    @Override
    public boolean isValue() {
        return true;
    }

    @Override
    public ValueVariable asValue() {
        return this;
    }

    @Override
    public FunctionalIterator<AlphaEquivalence> alphaEquals(ValueVariable that) {
        return AlphaEquivalence.empty()
                .alphaEqualIf(id().reference().isNameValue() == that.id().reference().isNameValue())
                .map(a -> a.extend(this, that));
    }
}
