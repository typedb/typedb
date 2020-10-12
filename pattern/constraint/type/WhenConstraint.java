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

package grakn.core.pattern.constraint.type;

import grakn.core.pattern.variable.TypeVariable;
import graql.lang.pattern.Pattern;

import java.util.Objects;
import java.util.Set;

import static grakn.common.collection.Collections.set;

// TODO: Move this out of TypeConstraint and create its own class
public class WhenConstraint extends TypeConstraint {

    private final Pattern pattern;
    private final int hash;

    private WhenConstraint(final TypeVariable owner, final Pattern pattern) {
        super(owner);
        if (pattern == null) throw new NullPointerException("Null Pattern");
        this.pattern = pattern;
        this.hash = Objects.hash(WhenConstraint.class, this.owner, this.pattern);
    }

    public static WhenConstraint of(final TypeVariable owner, final graql.lang.pattern.constraint.TypeConstraint.When constraint) {
        return new WhenConstraint(owner, constraint.pattern());
    }

    public Pattern pattern() {
        return pattern;
    }

    @Override
    public Set<TypeVariable> variables() {
        return set();
    }


    @Override
    public boolean isWhen() {
        return true;
    }

    @Override
    public WhenConstraint asWhen() {
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final WhenConstraint that = (WhenConstraint) o;
        return (this.owner.equals(that.owner) && this.pattern.equals(that.pattern));
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
