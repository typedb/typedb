/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.pattern.constraint;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.constraint.thing.ThingConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.TypeConstraint;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.traversal.GraphTraversal;

import java.util.Set;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public abstract class Constraint {

    public abstract Variable owner();

    public abstract Set<? extends Variable> variables();

    public abstract void addTo(GraphTraversal traversal);

    public boolean isType() {
        return false;
    }

    public boolean isThing() {
        return false;
    }

    public TypeConstraint asType() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(TypeConstraint.class));
    }

    public ThingConstraint asThing() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(ThingConstraint.class));
    }

    @Override
    public abstract boolean equals(Object o);

    @Override
    public abstract int hashCode();

    public abstract Constraint clone(Conjunction.Cloner constraintCloner);
}
