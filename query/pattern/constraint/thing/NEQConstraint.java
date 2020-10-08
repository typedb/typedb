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

package grakn.core.query.pattern.constraint.thing;

import grakn.core.query.pattern.variable.ThingVariable;
import grakn.core.query.pattern.variable.Variable;
import grakn.core.query.pattern.variable.VariableRegistry;

import java.util.Objects;
import java.util.Set;

import static grakn.common.collection.Collections.set;

public class NEQConstraint extends ThingConstraint {

    private final ThingVariable variable;
    private final int hash;

    private NEQConstraint(final ThingVariable owner, final ThingVariable variable) {
        super(owner);
        this.variable = variable;
        this.hash = Objects.hash(NEQConstraint.class, this.owner, this.variable);
    }

    public static NEQConstraint of(final ThingVariable owner,
                                   final graql.lang.pattern.constraint.ThingConstraint.NEQ constraint,
                                   final VariableRegistry registry) {
        return new NEQConstraint(owner, registry.register(constraint.variable()));
    }

    public ThingVariable variable() {
        return variable;
    }

    @Override
    public Set<Variable> variables() {
        return set(variable());
    }

    @Override
    public boolean isNEQ() {
        return true;
    }

    @Override
    public NEQConstraint asNEQ() {
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final NEQConstraint that = (NEQConstraint) o;
        return (this.owner.equals(that.owner) && this.variable.equals(that.variable));
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
