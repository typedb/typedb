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

import grakn.core.pattern.Conjunction;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.VariableCloner;
import grakn.core.pattern.variable.VariableRegistry;
import grakn.core.traversal.Traversal;
import graql.lang.pattern.constraint.ConceptConstraint;

import java.util.Objects;

import static grakn.common.collection.Collections.set;
import static graql.lang.common.GraqlToken.Char.SPACE;
import static graql.lang.common.GraqlToken.Constraint.IS;

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
    public void addTo(Traversal traversal) {
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
        return "" + IS + SPACE + variable.reference().toString();
    }

    @Override
    public IsConstraint clone(Conjunction.Cloner cloner) {
        return cloner.cloneVariable(owner).is(cloner.cloneVariable(variable));
    }
}
