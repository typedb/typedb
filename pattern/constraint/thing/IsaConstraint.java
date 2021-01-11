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

import grakn.core.pattern.equivalence.AlphaEquivalence;
import grakn.core.pattern.equivalence.AlphaEquivalent;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.VariableCloner;
import grakn.core.pattern.variable.VariableRegistry;
import grakn.core.traversal.Traversal;

import java.util.Objects;

import static grakn.common.collection.Collections.set;
import static graql.lang.common.GraqlToken.Char.SPACE;
import static graql.lang.common.GraqlToken.Constraint.ISA;
import static graql.lang.common.GraqlToken.Constraint.ISAX;

public class IsaConstraint extends ThingConstraint implements AlphaEquivalent<IsaConstraint> {

    private final TypeVariable type;
    private final boolean isExplicit;
    private final int hash;

    public IsaConstraint(ThingVariable owner, TypeVariable type, boolean isExplicit) {
        super(owner, set(type));
        this.type = type;
        this.isExplicit = isExplicit;
        this.hash = Objects.hash(IsaConstraint.class, this.owner, this.type, this.isExplicit);
        type.constraining(this);
    }

    public static IsaConstraint of(ThingVariable owner, graql.lang.pattern.constraint.ThingConstraint.Isa constraint,
                                   VariableRegistry registry) {
        return new IsaConstraint(owner, registry.register(constraint.type()), constraint.isExplicit());
    }

    public static IsaConstraint of(ThingVariable owner, IsaConstraint clone, VariableCloner cloner) {
        return new IsaConstraint(owner, cloner.clone(clone.type()), clone.isExplicit());
    }

    public TypeVariable type() {
        return type;
    }

    public boolean isExplicit() {
        return isExplicit;
    }

    @Override
    public void addTo(Traversal traversal) {
        // TODO: assert !(type.reference().isLabel() && typeHints.isEmpty());
        if (type.reference().isName() || owner.resolvedTypes().isEmpty()) {
            traversal.isa(owner.id(), type.id(), !isExplicit);
        } else {
            assert !owner.resolvedTypes().isEmpty();
        }
    }

    @Override
    public boolean isIsa() {
        return true;
    }

    @Override
    public IsaConstraint asIsa() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final IsaConstraint that = (IsaConstraint) o;
        return (this.owner.equals(that.owner) &&
                this.type.equals(that.type) &&
                this.isExplicit == that.isExplicit);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return "" + (isExplicit ? ISAX : ISA) + SPACE + type.referenceSyntax();
    }

    @Override
    public AlphaEquivalence alphaEquals(IsaConstraint that) {
        return AlphaEquivalence.valid()
                .validIf(isExplicit() == that.isExplicit())
                .validIfAlphaEqual(type, that.type);
    }
}
