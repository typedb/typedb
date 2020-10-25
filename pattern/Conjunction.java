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
import grakn.core.common.exception.GraknException;
import grakn.core.pattern.variable.Variable;
import grakn.core.pattern.variable.VariableRegistry;
import grakn.core.traversal.Traversal;
import graql.lang.pattern.Conjunctable;
import graql.lang.pattern.variable.BoundVariable;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;
import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.UNBOUNDED_NEGATION;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class Conjunction implements Pattern {

    private static final String TRACE_PREFIX = "conjunction.";
    private final Set<Variable> variables;
    private final Set<Negation> negations;

    public Conjunction(final Set<Variable> variables, final Set<Negation> negations) {
        this.variables = variables;
        this.negations = negations;
    }

    public static Conjunction create(final graql.lang.pattern.Conjunction<Conjunctable> graql) {
        return create(graql, null);
    }

    public static Conjunction create(final graql.lang.pattern.Conjunction<Conjunctable> graql,
                                     @Nullable final VariableRegistry bounds) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "create")) {
            List<BoundVariable> graqlVariables = new ArrayList<>();
            List<graql.lang.pattern.Negation<?>> graqlNegations = new ArrayList<>();

            graql.patterns().forEach(conjunctable -> {
                if (conjunctable.isVariable()) graqlVariables.add(conjunctable.asVariable());
                else if (conjunctable.isNegation()) graqlNegations.add(conjunctable.asNegation());
                else throw GraknException.of(ILLEGAL_STATE);
            });

            if (graqlVariables.isEmpty() && !graqlNegations.isEmpty()) {
                throw GraknException.of(UNBOUNDED_NEGATION);
            }

            VariableRegistry registry = VariableRegistry.createFromVariables(graqlVariables, bounds);
            Set<Negation> graknNegations = graqlNegations.isEmpty() ? set() :
                    graqlNegations.stream().map(n -> Negation.create(n, registry)).collect(toSet());
            return new Conjunction(registry.variables(), graknNegations);
        }
    }

    public Set<Variable> variables() {
        return variables;
    }

    public Set<Negation> negations() {
        return negations;
    }

    public List<Traversal> traversals() {
        return variables.stream().flatMap(variable -> variable.traversals().stream()).collect(toList());
    }
}
