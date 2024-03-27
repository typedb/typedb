/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.pattern.constraint;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.constraint.value.ValueConstraint;
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

    public abstract void addTo(GraphTraversal.Thing traversal);

    public boolean isType() {
        return false;
    }

    public boolean isThing() {
        return false;
    }

    public boolean isValue() { return false; }

    public TypeConstraint asType() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(TypeConstraint.class));
    }

    public ThingConstraint asThing() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(ThingConstraint.class));
    }

    public ValueConstraint asValue() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(ValueConstraint.class));
    }

    @Override
    public abstract boolean equals(Object o);

    @Override
    public abstract int hashCode();

    public abstract Constraint clone(Conjunction.ConstraintCloner constraintCloner);
}
