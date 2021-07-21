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
        return new IsConstraint(owner, registry.register(constraint.variable()).asThing());
    }

    static IsConstraint of(ThingVariable owner, IsConstraint clone, VariableCloner cloner) {
        return new IsConstraint(owner, cloner.clone(clone.variable()));
    }

    public ThingVariable variable() {
        return variable;
    }

    @Override
    public void addTo(GraphTraversal traversal) {
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
    public IsConstraint clone(Conjunction.Cloner cloner) {
        return cloner.cloneVariable(owner).is(cloner.cloneVariable(variable));
    }
}
