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

package grakn.core.pattern;

import grabl.tracing.client.GrablTracingThreadStatic.ThreadTrace;
import grakn.common.collection.Either;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Label;
import grakn.core.pattern.constraint.Constraint;
import grakn.core.pattern.constraint.thing.IIDConstraint;
import grakn.core.pattern.constraint.type.LabelConstraint;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.Variable;
import grakn.core.pattern.variable.VariableCloner;
import grakn.core.pattern.variable.VariableRegistry;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.common.Identifier;
import graql.lang.pattern.Conjunctable;
import graql.lang.pattern.variable.BoundVariable;
import graql.lang.pattern.variable.Reference;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;
import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.UNBOUNDED_NEGATION;
import static grakn.core.common.exception.ErrorMessage.ThingRead.CONTRADICTORY_BOUND_VARIABLE;
import static grakn.core.common.iterator.Iterators.iterate;
import static graql.lang.common.GraqlToken.Char.CURLY_CLOSE;
import static graql.lang.common.GraqlToken.Char.CURLY_OPEN;
import static graql.lang.common.GraqlToken.Char.SEMICOLON;
import static graql.lang.common.GraqlToken.Char.SPACE;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;

public class Conjunction implements Pattern, Cloneable {

    private static final String TRACE_PREFIX = "conjunction.";
    private final Map<Identifier.Variable, Variable> variableMap;
    private final Set<Variable> variableSet;
    private final Set<Negation> negations;
    private final int hash;

    private boolean isSatisfiable;
    private boolean isBounded;

    public Conjunction(Set<Variable> variables, Set<Negation> negations) {
        this.variableSet = unmodifiableSet(variables);
        this.variableMap = parseToMap(variables);
        this.negations = unmodifiableSet(negations);
        this.hash = Objects.hash(variables, negations);
        this.isSatisfiable = true;
        this.isBounded = false;
    }

    private Map<Identifier.Variable, Variable> parseToMap(Set<Variable> variables) {
        HashMap<Identifier.Variable, Variable> map = new HashMap<>();
        iterate(variables).forEachRemaining(v -> map.put(v.id(), v));
        return unmodifiableMap(map);
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

    public void bound(Map<Reference.Name, Either<Label, byte[]>> bounds) {
        variableSet.forEach(var -> {
            if (var.id().isName() && bounds.containsKey(var.id().reference().asName())) {
                Either<Label, byte[]> boundVar = bounds.get(var.id().reference().asName());
                if (var.isType() != boundVar.isFirst()) throw GraknException.of(CONTRADICTORY_BOUND_VARIABLE, var);
                else if (var.isType()) {
                    Optional<LabelConstraint> existingLabel = var.asType().label();
                    if (existingLabel.isPresent() && !existingLabel.get().properLabel().equals(boundVar.first())) {
                        var.setSatisfiable(false);
                        this.setSatisfiable(false);
                    } else if (!existingLabel.isPresent()) {
                        var.asType().label(boundVar.first());
                        var.asType().setResolvedTypes(set(boundVar.first()));
                    }
                } else if (var.isThing()) {
                    Optional<IIDConstraint> existingIID = var.asThing().iid();
                    if (existingIID.isPresent() && !Arrays.equals(existingIID.get().iid(), (boundVar.second()))) {
                        var.setSatisfiable(false);
                        this.setSatisfiable(false);
                    } else {
                        var.asThing().iid(boundVar.second());
                    }
                }
                else throw GraknException.of(ILLEGAL_STATE);
            }
        });
        isBounded = true;
    }

    public Variable variable(Identifier.Variable identifier) {
        return variableMap.get(identifier);
    }

    public Set<Variable> variables() {
        return variableSet;
    }

    public Set<Negation> negations() {
        return negations;
    }

    public Traversal traversal(Set<Identifier.Variable.Name> filter) {
        Traversal traversal = new Traversal();
        variableSet.forEach(variable -> variable.addTo(traversal));
        assert iterate(filter).allMatch(variableMap::containsKey);
        traversal.filter(filter);
        return traversal;
    }

    public Traversal traversal() {
        return traversal(new HashSet<>());
    }

    public void setSatisfiable(boolean isSatisfiable) {
        this.isSatisfiable = isSatisfiable;
    }

    public boolean isSatisfiable() {
        return isSatisfiable;
    }

    public boolean isBounded() {
        return isBounded;
    }

    private boolean printable(Variable variable) {
        if (variable.reference().isName() || !variable.reference().isLabel()) return !variable.constraints().isEmpty();
        if (variable.isThing()) return !variable.asThing().relation().isEmpty() && !variable.asThing().has().isEmpty();
        if (variable.isType() && variable.reference().isLabel()) return variable.constraints().size() > 1;
        throw GraknException.of(ILLEGAL_STATE);
    }

    @Override
    public Conjunction clone() {
        return new Conjunction(VariableCloner.cloneFromConjunction(this).variables(),
                               iterate(this.negations).map(Negation::clone).toSet());
    }

    @Override
    public String toString() {
        return variableSet.stream().flatMap(variable -> variable.constraints().stream()).map(Object::toString)
                .collect(Collectors.joining("" + SEMICOLON + SPACE,
                                            "" + CURLY_OPEN + SPACE,
                                            "" + SEMICOLON + SPACE + CURLY_CLOSE));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || obj.getClass() != getClass()) return false;
        Conjunction that = (Conjunction) obj;
        // TODO: This doesn't work! It doesn't compare constraints
        return (this.variableSet.equals(that.variables()) &&
                this.negations.equals(that.negations()));
    }

    @Override
    public int hashCode() {
        return hash;
    }

    public static class Cloner {

        private final Map<Identifier.Variable, Variable> variables;
        private final Map<Constraint, Constraint> constraints;

        public Cloner() {
            variables = new HashMap<>();
            constraints = new HashMap<>();
        }

        public static Cloner cloneExactly(Set<? extends Constraint> s1, Constraint... s2) {
            LinkedHashSet<Constraint> ordered = new LinkedHashSet<>(s1);
            Collections.addAll(ordered, s2);
            return cloneExactly(ordered);
        }

        public static Cloner cloneExactly(Set<? extends Constraint> s1, Set<? extends Constraint> s2, Constraint... s3) {
            LinkedHashSet<Constraint> ordered = new LinkedHashSet<>(s1);
            ordered.addAll(s2);
            Collections.addAll(ordered, s3);
            return cloneExactly(ordered);
        }

        public static Cloner cloneExactly(Constraint constraint) {
            LinkedHashSet<Constraint> orderedSet = new LinkedHashSet<>();
            orderedSet.add(constraint);
            return cloneExactly(orderedSet);
        }

        private static Cloner cloneExactly(LinkedHashSet<? extends Constraint> constraints) {
            Cloner cloner = new Cloner();
            constraints.forEach(cloner::clone);
            return cloner;
        }

        private void clone(Constraint constraint) {
            constraints.put(constraint, constraint.clone(this));
        }

        public ThingVariable cloneVariable(ThingVariable variable) {
            return variables.computeIfAbsent(variable.id(), identifier -> {
                ThingVariable clone = new ThingVariable(identifier);
                clone.addResolvedTypes(variable.resolvedTypes());
                return clone;
            }).asThing();
        }

        public TypeVariable cloneVariable(TypeVariable variable) {
            return variables.computeIfAbsent(variable.id(), identifier -> {
                TypeVariable clone = new TypeVariable(identifier);
                clone.addResolvedTypes(variable.resolvedTypes());
                return clone;
            }).asType();
        }

        public Conjunction conjunction() {
            return new Conjunction(set(variables.values()), set());
        }

        public Constraint getClone(Constraint constraint) {
            return constraints.get(constraint);
        }
    }
}
