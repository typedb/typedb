/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.pattern.constraint.value;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.constraint.Constraint;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.ValueVariable;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.pattern.variable.VariableCloner;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;
import com.vaticle.typedb.core.traversal.GraphTraversal;
import com.vaticle.typeql.lang.common.TypeQLToken;
import com.vaticle.typeql.lang.common.TypeQLVariable;
import com.vaticle.typeql.lang.pattern.expression.Expression;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.concatToSet;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.VALUE_VARIABLE_DUPLICATE_ASSIGMENT;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.Iterators.link;

public class AssignmentConstraint extends ValueConstraint {

    private final Expression expression;
    private final Set<ThingVariable> thingArguments;
    private final Set<ValueVariable> valueArguments;
    private com.vaticle.typedb.core.traversal.expression.Expression<?> traversalExpression;
    private final int hash;

    public AssignmentConstraint(
            ValueVariable owner, Expression expression, Set<ThingVariable> thingArguments, Set<ValueVariable> valueArguments
    ) {
        super(owner, concatToSet(thingArguments, valueArguments));
        this.expression = expression;
        this.thingArguments = thingArguments;
        this.valueArguments = valueArguments;
        this.hash = Objects.hash(this.getClass(), this.owner, this.expression);
        variables().forEach(var -> var.constraining(this));
    }

    public static AssignmentConstraint of(ValueVariable valueVariable, com.vaticle.typeql.lang.pattern.constraint.ValueConstraint.Assignment constraint,
                                          VariableRegistry registry) {
        registry.bounds().ifPresent(bounds -> {
            if (bounds.isBound(valueVariable.reference())) {
                throw TypeDBException.of(VALUE_VARIABLE_DUPLICATE_ASSIGMENT, valueVariable.id());
            }
        });

        Set<ThingVariable> thingArguments = new HashSet<>();
        Set<ValueVariable> valueArguments = new HashSet<>();
        for (TypeQLVariable v : constraint.variables()) {
            if (v.isConceptVar()) thingArguments.add(registry.registerThingVariable(v.asConceptVar()));
            else if (v.isValueVar()) valueArguments.add(registry.registerValueVariable(v.asValueVar()));
            else throw TypeDBException.of(ILLEGAL_STATE);
        }
        return new AssignmentConstraint(valueVariable, constraint.expression(), thingArguments, valueArguments);
    }

    public static AssignmentConstraint of(ValueVariable valueVariable, AssignmentConstraint constraint, VariableCloner cloner) {
        Set<ThingVariable> clonedThingArgs = iterate(constraint.thingArguments).map(cloner::clone).toSet();
        Set<ValueVariable> clonedValueArgs = iterate(constraint.valueArguments).map(cloner::clone).toSet();
        AssignmentConstraint clone = new AssignmentConstraint(valueVariable, constraint.expression(), clonedThingArgs, clonedValueArgs);
        clone.traversalExpression(constraint.traversalExpression());
        return clone;
    }

    public Expression expression() {
        return expression;
    }

    public Encoding.ValueType<?> valueType() {
        return traversalExpression != null ? traversalExpression.returnType() : null;
    }

    public Set<ThingVariable> thingArguments() {
        return thingArguments;
    }

    public Set<ValueVariable> valueArguments() {
        return valueArguments;
    }

    public FunctionalIterator<? extends Variable> arguments() {
        return link(iterate(thingArguments), iterate(valueArguments));
    }

    public com.vaticle.typedb.core.traversal.expression.Expression<?> traversalExpression() {
        return traversalExpression;
    }

    public void traversalExpression(com.vaticle.typedb.core.traversal.expression.Expression<?> expression) {
        this.traversalExpression = expression;
    }

    @Override
    public void addTo(GraphTraversal.Thing traversal) {
        assert traversalExpression != null;
        traversal.assignment(
                owner.id(), iterate(thingArguments).map(ThingVariable::id),
                iterate(valueArguments).map(ValueVariable::id), traversalExpression
        );
    }

    @Override
    public boolean isAssignment() {
        return true;
    }

    @Override
    public AssignmentConstraint asAssignment() {
        return this;
    }

    @Override
    public String toString() {
        return owner().toString() + TypeQLToken.Char.SPACE + TypeQLToken.Char.EQUAL + TypeQLToken.Char.SPACE + expression.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AssignmentConstraint that = (AssignmentConstraint) o;
        return (this.owner.equals(that.owner) && this.expression.equals(that.expression));
    }

    @Override
    public int hashCode() {
        return this.hash;
    }

    @Override
    public Constraint clone(Conjunction.ConstraintCloner cloner) {
        Set<ThingVariable> clonedThingArgs = new HashSet<>();
        Set<ValueVariable> clonedValueArgs = new HashSet<>();
        thingArguments.forEach(v -> clonedThingArgs.add(cloner.cloneVariable(v)));
        valueArguments.forEach(v -> clonedValueArgs.add(cloner.cloneVariable(v)));
        AssignmentConstraint cloned = cloner.cloneVariable(this.owner).assign(expression, clonedThingArgs, clonedValueArgs);
        cloned.traversalExpression(traversalExpression);
        return cloned;
    }
}
