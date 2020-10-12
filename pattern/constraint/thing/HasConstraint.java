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
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.Variable;
import grakn.core.pattern.variable.VariableRegistry;

import java.util.Objects;
import java.util.Set;

import static grakn.common.collection.Collections.set;

public class HasConstraint extends ThingConstraint {

    private final TypeVariable type;
    private final ThingVariable attribute;
    private final int hash;

    private HasConstraint(final ThingVariable owner, final TypeVariable type, final ThingVariable attribute) {
        super(owner);
        assert type != null && attribute != null;
        this.type = type;
        this.attribute = attribute;
        this.hash = Objects.hash(HasConstraint.class, this.owner, this.type, this.attribute);
    }

    public static HasConstraint of(final ThingVariable owner,
                                   final graql.lang.pattern.constraint.ThingConstraint.Has constraint,
                                   final VariableRegistry register) {
        return new HasConstraint(owner, register.register(constraint.type()), register.register(constraint.attribute()));
    }

    public TypeVariable type() {
        return type;
    }

    public ThingVariable attribute() {
        return attribute;
    }

    @Override
    public Set<Variable> variables() {
        return set(attribute);
    }

    @Override
    public boolean isHas() {
        return true;
    }

    @Override
    public HasConstraint asHas() {
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final HasConstraint that = (HasConstraint) o;
        return (this.owner.equals(that.owner) &&
                this.type.equals(that.type) &&
                this.attribute.equals(that.attribute));
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
