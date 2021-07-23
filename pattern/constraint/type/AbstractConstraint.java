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
    public void addTo(GraphTraversal traversal) {
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
    public AbstractConstraint clone(Conjunction.Cloner cloner) {
        return cloner.cloneVariable(owner).setAbstract();
    }
}
