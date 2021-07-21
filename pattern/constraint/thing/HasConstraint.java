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

package com.vaticle.typedb.core.pattern.constraint.thing;

import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.equivalence.AlphaEquivalence;
import com.vaticle.typedb.core.pattern.equivalence.AlphaEquivalent;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.VariableCloner;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;
import com.vaticle.typedb.core.traversal.GraphTraversal;

import java.util.Objects;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.SPACE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Constraint.HAS;

public class HasConstraint extends ThingConstraint implements AlphaEquivalent<HasConstraint> {

    private final ThingVariable attribute;
    private final int hash;

    public HasConstraint(ThingVariable owner, ThingVariable attribute) {
        super(owner, set(attribute));
        assert attribute != null;
        this.attribute = attribute;
        this.hash = Objects.hash(HasConstraint.class, this.owner, this.attribute);
        attribute.constraining(this);
    }

    static HasConstraint of(ThingVariable owner, com.vaticle.typeql.lang.pattern.constraint.ThingConstraint.Has constraint,
                            VariableRegistry register) {
        return new HasConstraint(owner, register.register(constraint.attribute()));
    }

    static HasConstraint of(ThingVariable owner, HasConstraint clone, VariableCloner cloner) {
        return new HasConstraint(owner, cloner.clone(clone.attribute()));
    }

    public ThingVariable attribute() {
        return attribute;
    }

    @Override
    public void addTo(GraphTraversal traversal) {
        traversal.has(owner.id(), attribute.id());
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HasConstraint that = (HasConstraint) o;
        return (this.owner.equals(that.owner) && this.attribute.equals(that.attribute));
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return owner.toString() + SPACE + HAS + SPACE + attribute.toString();
    }

    @Override
    public AlphaEquivalence alphaEquals(HasConstraint that) {
        return AlphaEquivalence.valid().validIfAlphaEqual(attribute, that.attribute);
    }

    @Override
    public HasConstraint clone(Conjunction.Cloner cloner) {
        return cloner.cloneVariable(owner).has(cloner.cloneVariable(attribute));
    }
}
