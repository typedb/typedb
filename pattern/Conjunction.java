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

package grakn.core.pattern;

import grabl.tracing.client.GrablTracingThreadStatic.ThreadTrace;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.pattern.constraint.Constraint;
import grakn.core.pattern.variable.Identifier;
import grakn.core.pattern.variable.Variable;
import grakn.core.traversal.Traversal;
import graql.lang.pattern.Conjunctable;
import graql.lang.pattern.variable.BoundVariable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;
import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static java.util.stream.Collectors.toSet;

public class Conjunction implements Pattern {

    private static final String TRACE_PREFIX = "conjunction.";
    private final Set<Constraint> constraints;
    private final Set<Negation> negations;

    public Conjunction(final Set<Constraint> constraints, final Set<Negation> negations) {
        this.constraints = constraints;
        this.negations = negations;
    }

    public static Conjunction create(final graql.lang.pattern.Conjunction<Conjunctable> graql) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "create")) {
            List<BoundVariable> graqlVariables = new ArrayList<>();
            List<graql.lang.pattern.Negation<?>> graqlNegations = new ArrayList<>();

            graql.patterns().forEach(conjunctable -> {
                if (conjunctable.isVariable()) graqlVariables.add(conjunctable.asVariable());
                else if (conjunctable.isNegation()) graqlNegations.add(conjunctable.asNegation());
                else throw GraknException.of(ILLEGAL_STATE);
            });

            if (graqlVariables.isEmpty() && !graqlNegations.isEmpty()) {
                throw GraknException.of(ErrorMessage.Query.UNBOUNDED_NEGATION);
            }

            Set<Negation> graknNegations;
            Set<Variable> graknVariables = Variable.createFromVariables(graqlVariables);
            Set<Constraint> graknConstraints = graknVariables.stream()
                    .flatMap(v -> v.constraints().stream()).collect(toSet());

            if (!graqlNegations.isEmpty()) {
                Set<Identifier> bounds = graknVariables.stream().map(Variable::identifier)
                        .filter(id -> id.reference().isName()).collect(toSet());
                graknNegations = graqlNegations.stream()
                        .map(n -> Negation.create(n.normalise(), bounds)).collect(toSet());
            } else {
                graknNegations = set();
            }
            return new Conjunction(graknConstraints, graknNegations);
        }
    }

    public Set<Constraint> constraints() {
        return constraints;
    }

    public Set<Variable> variables() {
        return constraints.stream().flatMap(constraint -> constraint.variables().stream()).collect(toSet());
    }

    public Set<Negation> negations() {
        return negations;
    }

    public List<Traversal> traversals() {
        return null;
    }
}
