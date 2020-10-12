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
 */

package grakn.core.pattern.constraint.thing;

import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.Variable;
import grakn.core.pattern.variable.VariableRegistry;

import java.util.Objects;
import java.util.Set;

import static grakn.common.collection.Collections.set;

public class ValueConstraint<T> extends ThingConstraint {

    private final ValueOperation<T> operation;
    private final int hash;

    private ValueConstraint(final ThingVariable owner, final ValueOperation<T> operation) {
        super(owner);
        this.operation = operation;
        this.hash = Objects.hash(ValueConstraint.class, this.owner, this.operation);
    }

    public static ValueConstraint<?> of(final ThingVariable owner,
                                        final graql.lang.pattern.constraint.ThingConstraint.Value<?> constraint,
                                        final VariableRegistry registry) {
        return new ValueConstraint<>(owner, ValueOperation.of(constraint.operation(), registry));
    }

    public ValueOperation<T> operation() {
        return operation;
    }

    @Override
    public Set<Variable> variables() {
        return operation.variable().isPresent() ? set(operation.variable().get()) : set();
    }

    @Override
    public boolean isValue() {
        return true;
    }

    @Override
    public ValueConstraint<?> asValue() {
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ValueConstraint<?> that = (ValueConstraint<?>) o;
        return (this.owner.equals(that.owner) && this.operation.equals(that.operation));
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
