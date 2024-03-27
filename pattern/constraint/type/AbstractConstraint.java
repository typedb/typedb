/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.pattern.constraint.type;

import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;
import com.vaticle.typedb.core.traversal.GraphTraversal;

import java.util.Objects;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.SPACE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Constraint.ABSTRACT;

public class AbstractConstraint extends TypeConstraint {

    private final int hash;

    public AbstractConstraint(TypeVariable owner) {
        super(owner, set());
        this.hash = Objects.hash(AbstractConstraint.class, this.owner);
    }

    static AbstractConstraint of(TypeVariable owner) {
        return new AbstractConstraint(owner);
    }

    @Override
    public void addTo(GraphTraversal.Thing traversal) {
        traversal.isAbstract(owner.id());
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
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractConstraint that = (AbstractConstraint) o;
        return this.owner.equals(that.owner);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return owner.toString() + SPACE + ABSTRACT;
    }

    @Override
    public AbstractConstraint clone(Conjunction.ConstraintCloner cloner) {
        return cloner.cloneVariable(owner).setAbstract();
    }
}
