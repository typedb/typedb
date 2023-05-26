/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.pattern.constraint.value;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.pattern.constraint.Constraint;
import com.vaticle.typedb.core.pattern.variable.ValueVariable;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.pattern.variable.VariableCloner;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public abstract class ValueConstraint extends Constraint {
    protected final ValueVariable owner;
    private final Set<Variable> variables;

    public ValueConstraint(ValueVariable owner, Set<Variable> additionalVariables) {
        this.owner = owner;
        Set<Variable> vars = new HashSet<>(additionalVariables);
        vars.add(owner);
        this.variables = Collections.unmodifiableSet(vars);
    }

    public static ValueConstraint of(ValueVariable valueVariable, com.vaticle.typeql.lang.pattern.constraint.ValueConstraint constraint, VariableRegistry registry) {
        if (constraint.isAssignment()) {
            return AssignmentConstraint.of(valueVariable, constraint.asAssignment(), registry);
        } else if (constraint.isPredicate()) {
            return PredicateConstraint.of(valueVariable, constraint.asPredicate(), registry);
        } else throw TypeDBException.of(ILLEGAL_STATE);
    }

    public static ValueConstraint of(ValueVariable valueVariable, ValueConstraint constraint, VariableCloner cloner) {
        if (constraint.isAssignment()) {
            return AssignmentConstraint.of(valueVariable, constraint.asAssignment(), cloner);
        } else if (constraint.isPredicate()) {
            return PredicateConstraint.of(valueVariable, constraint.asPredicate(), cloner);
        } else throw TypeDBException.of(ILLEGAL_STATE);
    }

    @Override
    public Variable owner() {
        return owner;
    }

    @Override
    public Set<? extends Variable> variables() {
        return variables;
    }

    @Override
    public boolean isValue() {
        return true;
    }

    @Override
    public ValueConstraint asValue() {
        return this;
    }

    public boolean isPredicate() {
        return false;
    }

    public boolean isAssignment() {
        return false;
    }

    public PredicateConstraint asPredicate() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(PredicateConstraint.class));
    }

    public AssignmentConstraint asAssignment() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(AssignmentConstraint.class));
    }

    @Override
    public abstract boolean equals(Object o);

    @Override
    public abstract int hashCode();
}
