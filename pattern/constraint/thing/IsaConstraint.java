/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.pattern.constraint.thing;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.equivalence.AlphaEquivalence;
import com.vaticle.typedb.core.pattern.equivalence.AlphaEquivalent;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;
import com.vaticle.typedb.core.pattern.variable.VariableCloner;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;
import com.vaticle.typedb.core.traversal.GraphTraversal;

import java.util.Objects;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.SPACE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Constraint.ISA;
import static com.vaticle.typeql.lang.common.TypeQLToken.Constraint.ISAX;

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

    public static IsaConstraint of(ThingVariable owner, com.vaticle.typeql.lang.pattern.constraint.ThingConstraint.Isa constraint,
                                   VariableRegistry registry) {
        return new IsaConstraint(owner, registry.registerTypeVariable(constraint.type()), constraint.isExplicit());
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
    public void addTo(GraphTraversal.Thing traversal) {
        // TODO: assert !(type.reference().isLabel() && typeHints.isEmpty());
        if (type.reference().isName() || owner.inferredTypes().isEmpty()) {
            traversal.isa(owner.id(), type.id(), !isExplicit);
        } else {
            assert !owner.inferredTypes().isEmpty();
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
        IsaConstraint that = (IsaConstraint) o;
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
        return owner.toString() + SPACE + (isExplicit ? ISAX : ISA) + SPACE + type.toString();
    }

    @Override
    public FunctionalIterator<AlphaEquivalence> alphaEquals(IsaConstraint that) {
        return owner.alphaEquals(that.owner)
                .flatMap(a -> type.alphaEquals(that.type).flatMap(a::extendIfCompatible))
                .flatMap(a -> a.alphaEqualIf(isExplicit() == that.isExplicit()));
    }

    @Override
    public IsaConstraint clone(Conjunction.ConstraintCloner cloner) {
        return cloner.cloneVariable(owner).isa(cloner.cloneVariable(type), isExplicit);
    }
}
