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

package com.vaticle.typedb.core.pattern;

import com.vaticle.factory.tracing.client.FactoryTracingThreadStatic.ThreadTrace;
import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.pattern.constraint.Constraint;
import com.vaticle.typedb.core.pattern.constraint.thing.IIDConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.LabelConstraint;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.pattern.variable.VariableCloner;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;
import com.vaticle.typedb.core.traversal.GraphTraversal;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import com.vaticle.typeql.lang.pattern.Conjunctable;
import com.vaticle.typeql.lang.pattern.variable.BoundVariable;

import javax.annotation.Nullable;
import java.util.ArrayList;
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

import static com.vaticle.factory.tracing.client.FactoryTracingThreadStatic.traceOnThread;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.UNBOUNDED_NEGATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingRead.CONTRADICTORY_BOUND_VARIABLE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.CURLY_CLOSE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.CURLY_OPEN;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.NEW_LINE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.SEMICOLON;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.SPACE;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;

public class Conjunction implements Pattern, Cloneable {

    private static final String TRACE_PREFIX = "conjunction.";
    private final Map<Identifier.Variable, Variable> variableMap;
    private final Set<Variable> variableSet;
    private final Set<Negation> negations;
    private final int hash;

    private boolean isCoherent;
    private boolean isBounded;
    private Set<Identifier.Variable.Retrievable> retrieves;

    public Conjunction(Set<Variable> variables, Set<Negation> negations) {
        this.variableSet = unmodifiableSet(variables);
        this.variableMap = parseToMap(variables);
        this.negations = unmodifiableSet(negations);
        this.hash = Objects.hash(variables, negations);
        this.isCoherent = true;
        this.isBounded = false;
    }

    private Map<Identifier.Variable, Variable> parseToMap(Set<Variable> variables) {
        HashMap<Identifier.Variable, Variable> map = new HashMap<>();
        iterate(variables).forEachRemaining(v -> map.put(v.id(), v));
        return unmodifiableMap(map);
    }

    public static Conjunction create(com.vaticle.typeql.lang.pattern.Conjunction<Conjunctable> typeql) {
        return create(typeql, null);
    }

    public static Conjunction create(com.vaticle.typeql.lang.pattern.Conjunction<Conjunctable> typeql,
                                     @Nullable VariableRegistry bounds) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "create")) {
            List<BoundVariable> typeQLVariables = new ArrayList<>();
            List<com.vaticle.typeql.lang.pattern.Negation<?>> typeQLNegations = new ArrayList<>();

            typeql.patterns().forEach(conjunctable -> {
                if (conjunctable.isVariable()) typeQLVariables.add(conjunctable.asVariable());
                else if (conjunctable.isNegation()) typeQLNegations.add(conjunctable.asNegation());
                else throw TypeDBException.of(ILLEGAL_STATE);
            });

            if (typeQLVariables.isEmpty() && !typeQLNegations.isEmpty()) throw TypeDBException.of(UNBOUNDED_NEGATION);
            VariableRegistry registry = VariableRegistry.createFromVariables(typeQLVariables, bounds);
            Set<Negation> typeDBNegations = typeQLNegations.isEmpty() ? set() :
                    typeQLNegations.stream().map(n -> Negation.create(n, registry)).collect(toSet());
            return new Conjunction(registry.variables(), typeDBNegations);
        }
    }

    public void bound(Map<Retrievable, Either<Label, ByteArray>> bounds) {
        variableSet.forEach(var -> {
            if (var.id().isRetrievable() && bounds.containsKey(var.id().asRetrievable())) {
                Either<Label, ByteArray> boundVar = bounds.get(var.id().asRetrievable());
                if (var.isType() != boundVar.isFirst()) throw TypeDBException.of(CONTRADICTORY_BOUND_VARIABLE, var);
                else if (var.isType()) {
                    Optional<LabelConstraint> existingLabel = var.asType().label();
                    if (existingLabel.isPresent() && !existingLabel.get().properLabel().equals(boundVar.first())) {
                        this.setCoherent(false);
                    } else if (!existingLabel.isPresent()) {
                        var.asType().label(boundVar.first());
                        var.asType().setResolvedTypes(set(boundVar.first()));
                    }
                } else if (var.isThing()) {
                    Optional<IIDConstraint> existingIID = var.asThing().iid();
                    if (existingIID.isPresent() && !existingIID.get().iid().equals(boundVar.second())) {
                        this.setCoherent(false);
                    } else {
                        var.asThing().iid(boundVar.second());
                    }
                } else throw TypeDBException.of(ILLEGAL_STATE);
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

    public Set<Identifier.Variable> identifiers() {
        return variableMap.keySet();
    }

    public Set<Identifier.Variable.Retrievable> retrieves() {
        if (retrieves == null) {
            retrieves = iterate(identifiers())
                    .filter(Identifier::isRetrievable)
                    .map(Identifier.Variable::asRetrievable).toSet();
        }
        return retrieves;
    }

    public Set<Negation> negations() {
        return negations;
    }

    public GraphTraversal traversal(Set<? extends Retrievable> filter) {
        GraphTraversal traversal = new GraphTraversal();
        variableSet.forEach(variable -> variable.addTo(traversal));
        assert iterate(filter).allMatch(variableMap::containsKey);
        traversal.filter(filter);
        return traversal;
    }

    public GraphTraversal traversal() {
        return traversal(new HashSet<>());
    }

    public void setCoherent(boolean isCoherent) {
        this.isCoherent = isCoherent;
    }

    public boolean isCoherent() {
        return isCoherent && iterate(negations).allMatch(Negation::isCoherent);
    }

    public boolean isBounded() {
        return isBounded;
    }

    @Override
    public Conjunction clone() {
        return new Conjunction(VariableCloner.cloneFromConjunction(this).variables(),
                               iterate(this.negations).map(Negation::clone).toSet());
    }

    @Override
    public String toString() {
        String negationsToString = negations.isEmpty() ? "" : negations.stream().map(Object::toString).collect(
                Collectors.joining("" + SEMICOLON + SPACE, "", "" + SEMICOLON + SPACE));
        return variableSet.stream()
                .filter(v -> !v.id().isLabel())
                .map(variable -> variable.constraints().stream().map(Object::toString)
                        .collect(Collectors.joining("" + SEMICOLON + SPACE)))
                .filter(s -> !s.isEmpty())
                .collect(Collectors.joining("; " + NEW_LINE,
                                            "" + CURLY_OPEN + SPACE,
                                            "" + SEMICOLON + SPACE + negationsToString + CURLY_CLOSE));

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
