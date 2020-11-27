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
 */

package grakn.core.reasoner;

import grakn.core.concept.schema.Rule;
import grakn.core.pattern.constraint.Constraint;
import grakn.core.pattern.constraint.thing.ThingConstraint;
import grakn.core.pattern.variable.Variable;

import java.util.HashSet;
import java.util.Set;

public class Implication {

    private final Set<Concludable<?>> head;
    private final Set<Concludable<?>> body;
    private final Rule rule;

    public Implication(Rule rule) {
        this.rule = rule;
        head = createHead(rule.then(), rule.when().variables());
        body = Concludable.of(rule.when());
    }

    public Set<Concludable<?>> body() {
        return body;
    }

    public Set<Concludable<?>> head() {
        return head;
    }

    private Set<Concludable<?>> createHead(Set<Constraint> thenConstraints, Set<Variable> constraintContext) {
        HashSet<Concludable<?>> thenConcludables = new HashSet<>();
        thenConstraints.stream().filter(Constraint::isThing).map(Constraint::asThing).filter(ThingConstraint::isRelation).map(ThingConstraint::asRelation).findFirst().ifPresent(relationConstraint -> thenConcludables.add(new Concludable.Relation(relationConstraint, constraintContext)));
        thenConstraints.stream().filter(Constraint::isThing).map(Constraint::asThing).filter(ThingConstraint::isHas).map(ThingConstraint::asHas).findFirst().ifPresent(hasConstraint -> thenConcludables.add(new Concludable.Has(hasConstraint, constraintContext)));
        thenConstraints.stream().filter(Constraint::isThing).map(Constraint::asThing).filter(ThingConstraint::isIsa).map(ThingConstraint::asIsa).findFirst().ifPresent(isaConstraint -> thenConcludables.add(new Concludable.Isa(isaConstraint, constraintContext)));
        thenConstraints.stream().filter(Constraint::isThing).map(Constraint::asThing).filter(ThingConstraint::isValue).map(ThingConstraint::asValue).findFirst().ifPresent(valueConstraint -> thenConcludables.add(new Concludable.Value(valueConstraint, constraintContext)));
        return thenConcludables;
    }
}
