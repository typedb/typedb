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

package grakn.core.pattern.constraint.type;

import grakn.core.common.parameters.Label;
import grakn.core.pattern.constraint.ConstraintCloner;
import grakn.core.pattern.equivalence.AlphaEquivalence;
import grakn.core.pattern.equivalence.AlphaEquivalent;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.traversal.Traversal;

import java.util.Objects;
import java.util.Optional;

import static grakn.common.collection.Collections.set;
import static graql.lang.common.GraqlToken.Char.SPACE;
import static graql.lang.common.GraqlToken.Constraint.TYPE;

public class LabelConstraint extends TypeConstraint implements AlphaEquivalent<LabelConstraint> {

    private final Label label;
    private final int hash;

    public LabelConstraint(TypeVariable owner, Label label) {
        super(owner, set());
        if (label == null) throw new NullPointerException("Null label");
        this.label = label;
        this.hash = Objects.hash(LabelConstraint.class, this.owner, this.label);
    }

    static LabelConstraint of(TypeVariable owner, graql.lang.pattern.constraint.TypeConstraint.Label constraint) {
        return new LabelConstraint(owner, Label.of(constraint.label(), constraint.scope().orElse(null)));
    }

    static LabelConstraint of(TypeVariable owner, LabelConstraint clone) {
        return new LabelConstraint(owner, Label.of(clone.label(), clone.scope().orElse(null)));
    }

    public Optional<String> scope() {
        return label.scope();
    }

    public String label() {
        return label.name();
    }

    public String scopedLabel() {
        return label.scopedName();
    }

    public Label properLabel() {
        return label;
    }

    @Override
    public void addTo(Traversal traversal) {
        assert !owner.resolvedTypes().isEmpty();
    }

    @Override
    public boolean isLabel() {
        return true;
    }

    @Override
    public LabelConstraint asLabel() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final LabelConstraint that = (LabelConstraint) o;
        return this.owner.equals(that.owner) && this.label.equals(that.label);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() { return "" + TYPE + SPACE + scopedLabel(); }

    @Override
    public AlphaEquivalence alphaEquals(LabelConstraint that) {
        return AlphaEquivalence.valid().validIf(label().equals(that.label()));
    }

    @Override
    protected LabelConstraint clone(ConstraintCloner cloner) {
        return cloner.cloneVariable(owner).label(label);
    }
}
