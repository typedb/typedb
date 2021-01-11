/*
 * Copyright (C) 2021 Grakn Labs
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

import grakn.core.pattern.constraint.ConstraintCloner;
import grakn.core.pattern.equivalence.AlphaEquivalence;
import grakn.core.pattern.equivalence.AlphaEquivalent;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.VariableCloner;
import grakn.core.pattern.variable.VariableRegistry;
import grakn.core.traversal.Traversal;

import java.util.Objects;

import static grakn.common.collection.Collections.set;
import static graql.lang.common.GraqlToken.Char.SPACE;
import static graql.lang.common.GraqlToken.Constraint.HAS;

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

    static HasConstraint of(ThingVariable owner, graql.lang.pattern.constraint.ThingConstraint.Has constraint,
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
    public void addTo(Traversal traversal) {
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
        final HasConstraint that = (HasConstraint) o;
        return (this.owner.equals(that.owner) && this.attribute.equals(that.attribute));
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        StringBuilder syntax = new StringBuilder();
        syntax.append(HAS).append(SPACE);

        if (attribute.reference().isName()) {
            syntax.append(attribute.reference().toString());
        } else {
            if (attribute.isa().isPresent() && attribute.isa().get().type().label().isPresent()) {
                syntax.append(attribute.isa().get().type().label().get().label());
            }
            syntax.append(SPACE);
            attribute.value().forEach(value -> syntax.append(value.toString()));
        }

        return syntax.toString();
    }

    @Override
    public AlphaEquivalence alphaEquals(HasConstraint that) {
        return AlphaEquivalence.valid().validIfAlphaEqual(attribute, that.attribute);
    }

    @Override
    protected HasConstraint clone(ConstraintCloner cloner) {
        return cloner.cloneVariable(owner).has(cloner.cloneVariable(attribute));
    }
}
