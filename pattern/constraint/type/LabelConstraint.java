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

import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.equivalence.AlphaEquivalence;
import com.vaticle.typedb.core.pattern.equivalence.AlphaEquivalent;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;
import com.vaticle.typedb.core.traversal.GraphTraversal;

import java.util.Objects;
import java.util.Optional;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.SPACE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Constraint.TYPE;

public class LabelConstraint extends TypeConstraint implements AlphaEquivalent<LabelConstraint> {

    private final Label label;
    private final int hash;

    public LabelConstraint(TypeVariable owner, Label label) {
        super(owner, set());
        if (label == null) throw new NullPointerException("Null label");
        this.label = label;
        this.hash = Objects.hash(LabelConstraint.class, this.owner, this.label);
    }

    static LabelConstraint of(TypeVariable owner, com.vaticle.typeql.lang.pattern.constraint.TypeConstraint.Label constraint) {
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
    public void addTo(GraphTraversal traversal) {
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
        LabelConstraint that = (LabelConstraint) o;
        return this.owner.equals(that.owner) && this.label.equals(that.label);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return owner.toString() + SPACE + TYPE + SPACE + scopedLabel();
    }

    @Override
    public AlphaEquivalence alphaEquals(LabelConstraint that) {
        return AlphaEquivalence.valid().validIf(label().equals(that.label()));
    }

    @Override
    public LabelConstraint clone(Conjunction.Cloner cloner) {
        return cloner.cloneVariable(owner).label(label);
    }
}
