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

import grakn.core.common.parameters.Label;
import grakn.core.pattern.equivalence.AlphaEquivalence;
import grakn.core.pattern.equivalence.AlphaEquivalent;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.VariableRegistry;
import grakn.core.traversal.Traversal;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static grakn.common.collection.Collections.set;
import static graql.lang.common.GraqlToken.Char.SPACE;
import static graql.lang.common.GraqlToken.Constraint.ISA;
import static graql.lang.common.GraqlToken.Constraint.ISAX;

public class IsaConstraint extends ThingConstraint implements AlphaEquivalent<IsaConstraint> {

    private final TypeVariable type;
    private final boolean isExplicit;
    private final int hash;
    private final Set<Label> typeHints;

    public IsaConstraint(ThingVariable owner, TypeVariable type, boolean isExplicit) {
        super(owner, set(type));
        this.type = type;
        this.isExplicit = isExplicit;
        this.hash = Objects.hash(IsaConstraint.class, this.owner, this.type, this.isExplicit);
        this.typeHints = new HashSet<>();
    }

    public static IsaConstraint of(ThingVariable owner, graql.lang.pattern.constraint.ThingConstraint.Isa constraint,
                                   VariableRegistry registry) {
        return new IsaConstraint(owner, registry.register(constraint.type()), constraint.isExplicit());
    }

    public TypeVariable type() {
        return type;
    }

    public boolean isExplicit() {
        return isExplicit;
    }

    public void addHints(Set<Label> labels) {
        typeHints.addAll(labels);
    }

    public void retainHints(Set<Label> labels) { typeHints.retainAll(labels); }

    public void removeHint(Label label) {
        typeHints.remove(label);
    }

    public void clearHintLabels() {
        typeHints.clear();
    }

    public Set<Label> getTypeHints() {
        return typeHints;
    }

    @Override
    public void addTo(Traversal traversal) {
        // TODO: assert !(type.reference().isLabel() && typeHints.isEmpty());
        if (!typeHints.isEmpty()) traversal.types(owner.identifier(), typeHints);
        if (type.reference().isName() || typeHints.isEmpty()) {
            traversal.isa(owner.identifier(), type.identifier(), !isExplicit);
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
                .validIf(typeHints.equals(that.typeHints))
                .validIfAlphaEqual(type, that.type);
    }
}
