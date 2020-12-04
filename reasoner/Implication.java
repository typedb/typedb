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

import grakn.core.concept.logic.Rule;
import grakn.core.pattern.constraint.Constraint;
import grakn.core.pattern.variable.Variable;
import grakn.core.reasoner.concludable.ConjunctionConcludable;
import grakn.core.reasoner.concludable.HeadConcludable;

import java.util.HashSet;
import java.util.Set;

public class Implication {

    private final Set<HeadConcludable<?, ?>> head;
    private final Set<ConjunctionConcludable<?, ?>> body;
    private final Rule rule;

    public Implication(Rule rule) {
        this.rule = rule;
        head = createHead(rule.then(), rule.when().variables());
        body = ConjunctionConcludable.of(rule.when());
    }

    public Set<ConjunctionConcludable<?, ?>> body() {
        return body;
    }

    public Set<HeadConcludable<?, ?>> head() {
        return head;
    }

    private Set<HeadConcludable<?, ?>> createHead(Set<Constraint> thenConstraints, Set<Variable> constraintContext) {
        HashSet<HeadConcludable<?, ?>> thenConcludables = new HashSet<>();
        thenConstraints.stream().filter(Constraint::isThing).map(Constraint::asThing)
                .flatMap(constraint -> HeadConcludable.of(constraint, constraintContext).stream()).forEach(thenConcludables::add);
        return thenConcludables;
    }
}
