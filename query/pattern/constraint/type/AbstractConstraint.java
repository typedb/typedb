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

package grakn.core.query.pattern.constraint.type;

import grakn.core.query.pattern.variable.TypeVariable;

import java.util.Objects;
import java.util.Set;

import static grakn.common.collection.Collections.set;

public class AbstractConstraint extends TypeConstraint {

    private final int hash;

    private AbstractConstraint(final TypeVariable owner) {
        super(owner);
        this.hash = Objects.hash(AbstractConstraint.class, this.owner);
    }

    public static AbstractConstraint of(final TypeVariable owner) {
        return new AbstractConstraint(owner);
    }

    @Override
    public Set<TypeVariable> variables() {
        return set();
    }

    @Override
    public boolean isAbstract() {
        return true;
    }

    @Override
    public AbstractConstraint asAbstract() {
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final AbstractConstraint that = (AbstractConstraint) o;
        return this.owner.equals(that.owner);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
