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
import grakn.core.pattern.variable.VariableCloner;
import grakn.core.pattern.variable.VariableRegistry;
import grakn.core.traversal.Traversal;
import graql.lang.pattern.Conjunctable;
import graql.lang.pattern.variable.BoundVariable;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;
import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.UNBOUNDED_NEGATION;
import static grakn.core.common.iterator.Iterators.iterate;
import static graql.lang.common.GraqlToken.Char.NEW_LINE;
import static graql.lang.common.GraqlToken.Char.SEMICOLON;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;

public class Conjunction implements Pattern, Cloneable {

    private static final String TRACE_PREFIX = "conjunction.";
    private final Set<Variable> variables;
    private final Set<Negation> negations;
    private final int hash;

    public Conjunction(Set<Variable> variables, Set<Negation> negations) {
        this.variables = unmodifiableSet(variables);
        this.negations = unmodifiableSet(negations);
        this.hash = Objects.hash(variables, negations);
    }

    public static Conjunction create(graql.lang.pattern.Conjunction<Conjunctable> graql) {
        return create(graql, null);
    }

    public static Conjunction create(graql.lang.pattern.Conjunction<Conjunctable> graql,
                                     @Nullable VariableRegistry bounds) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "create")) {
            List<BoundVariable> graqlVariables = new ArrayList<>();
            List<graql.lang.pattern.Negation<?>> graqlNegations = new ArrayList<>();

            graql.patterns().forEach(conjunctable -> {
                if (conjunctable.isVariable()) graqlVariables.add(conjunctable.asVariable());
                else if (conjunctable.isNegation()) graqlNegations.add(conjunctable.asNegation());
                else throw GraknException.of(ILLEGAL_STATE);
            });

            if (graqlVariables.isEmpty() && !graqlNegations.isEmpty()) throw GraknException.of(UNBOUNDED_NEGATION);
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

    public Traversal traversal() {
        Traversal traversal = new Traversal();
        variables.forEach(variable -> variable.addTo(traversal));
        return traversal;
    }

    private boolean printable(Variable variable) {
        if (variable.reference().isName() || !variable.reference().isLabel()) return !variable.constraints().isEmpty();
        if (variable.isThing()) return !variable.asThing().relation().isEmpty() && !variable.asThing().has().isEmpty();
        if (variable.isType() && variable.reference().isLabel()) return variable.constraints().size() > 1;
        throw GraknException.of(ILLEGAL_STATE);
    }

    public void forEach(Consumer<Variable> function) {
        variables.forEach(function);
    }

    @Override
    public Conjunction clone() {
        return new Conjunction(VariableCloner.cloneFromConjunction(this).variables(),
                               iterate(this.negations).map(Negation::clone).toSet());
    }

    @Override
    public String toString() {
        return Stream.concat(variables.stream().filter(this::printable), negations.stream()).map(Pattern::toString)
                .collect(Collectors.joining("" + SEMICOLON + NEW_LINE, "", "" + SEMICOLON));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || obj.getClass() != getClass()) return false;
        final Conjunction that = (Conjunction) obj;
        // TODO This doesn't work! It doesn't compare constraints
        return (this.variables.equals(that.variables()) &&
                this.negations.equals(that.negations()));
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
