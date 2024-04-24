/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.pattern.constraint.thing;

import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.VariableCloner;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;
import com.vaticle.typedb.core.traversal.GraphTraversal;
import com.vaticle.typeql.lang.pattern.constraint.ConceptConstraint;

import java.util.Objects;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.SPACE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Constraint.IS;

public class IsConstraint extends ThingConstraint {

    private final ThingVariable variable;
    private final int hash;

    public IsConstraint(ThingVariable owner, ThingVariable variable) {
        super(owner, set(variable));
        this.variable = variable;
        this.hash = Objects.hash(IsConstraint.class, this.owner, this.variable);
        variable.constraining(this);
    }

    static IsConstraint of(ThingVariable owner, ConceptConstraint.Is constraint, VariableRegistry registry) {
        return new IsConstraint(owner, registry.registerThingVariable(constraint.variable()).asThing());
    }

    static IsConstraint of(ThingVariable owner, IsConstraint clone, VariableCloner cloner) {
        return new IsConstraint(owner, cloner.clone(clone.variable()));
    }

    public ThingVariable variable() {
        return variable;
    }

    @Override
    public void addTo(GraphTraversal.Thing traversal) {
        traversal.equalThings(owner.id(), variable.id());
    }

    @Override
    public boolean isIs() {
        return true;
    }

    @Override
    public IsConstraint asIs() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IsConstraint that = (IsConstraint) o;
        return (this.owner.equals(that.owner) && this.variable.equals(that.variable));
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return owner.toString() + SPACE + IS + SPACE + variable.toString();
    }

    @Override
    public IsConstraint clone(Conjunction.ConstraintCloner cloner) {
        return cloner.cloneVariable(owner).is(cloner.cloneVariable(variable));
    }
}
