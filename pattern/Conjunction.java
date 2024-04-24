/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.pattern;

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.iid.VertexIID;
import com.vaticle.typedb.core.pattern.constraint.Constraint;
import com.vaticle.typedb.core.pattern.constraint.thing.IIDConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.LabelConstraint;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;
import com.vaticle.typedb.core.pattern.variable.ValueVariable;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.pattern.variable.VariableCloner;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;
import com.vaticle.typedb.core.traversal.GraphTraversal;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Modifiers;
import com.vaticle.typeql.lang.pattern.Conjunctable;
import com.vaticle.typeql.lang.pattern.expression.Expression.Constant;
import com.vaticle.typeql.lang.pattern.statement.Statement;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.UNBOUNDED_NEGATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingRead.CONTRADICTORY_BOUND_VARIABLE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.BOOLEAN;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.DATETIME;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.DOUBLE;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.LONG;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.STRING;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.CURLY_CLOSE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.CURLY_OPEN;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.NEW_LINE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.SEMICOLON;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.SPACE;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toList;

public class Conjunction implements Pattern, Cloneable {

    private final Map<Identifier.Variable, Variable> variableMap;
    private final Set<Variable> variableSet;
    private final List<Negation> negations;
    private final int hash;

    private boolean isCoherent;
    private boolean isAnswerable;
    private Set<Identifier.Variable.Retrievable> retrieves;

    public Conjunction(Set<Variable> variables, List<Negation> negations) {
        this.variableSet = unmodifiableSet(variables);
        this.variableMap = parseToMap(variables);
        this.negations = unmodifiableList(negations);
        this.hash = Objects.hash(variables, negations);
        this.isCoherent = true;
        this.isAnswerable = true;
    }

    private Map<Identifier.Variable, Variable> parseToMap(Set<Variable> variables) {
        HashMap<Identifier.Variable, Variable> map = new HashMap<>();
        variables.forEach(v -> map.put(v.id(), v));
        return unmodifiableMap(map);
    }

    public static Conjunction create(com.vaticle.typeql.lang.pattern.Conjunction<Conjunctable> typeql) {
        return create(typeql, null);
    }

    public static Conjunction create(com.vaticle.typeql.lang.pattern.Conjunction<Conjunctable> typeql,
                                     @Nullable VariableRegistry bounds) {
        List<Statement> typeQLVariables = new ArrayList<>();
        List<com.vaticle.typeql.lang.pattern.Negation<?>> typeQLNegations = new ArrayList<>();

        typeql.patterns().forEach(conjunctable -> {
            if (conjunctable.isStatement()) typeQLVariables.add(conjunctable.asStatement());
            else if (conjunctable.isNegation()) typeQLNegations.add(conjunctable.asNegation());
            else throw TypeDBException.of(ILLEGAL_STATE);
        });

        if (typeQLVariables.isEmpty() && !typeQLNegations.isEmpty()) throw TypeDBException.of(UNBOUNDED_NEGATION);
        VariableRegistry registry = VariableRegistry.createFromStatements(typeQLVariables, bounds);
        List<Negation> typeDBNegations = typeQLNegations.isEmpty() ? list() :
                typeQLNegations.stream().map(n -> Negation.create(n, registry)).collect(toList());
        return new Conjunction(registry.variables(), typeDBNegations);
    }

    public void bound(Map<Identifier.Variable.Retrievable, Either<Label, ByteArray>> bounds) {
        variableSet.forEach(var -> {
            if (var.id().isRetrievable() && bounds.containsKey(var.id().asRetrievable())) {
                Either<Label, ByteArray> boundVar = bounds.get(var.id().asRetrievable());
                if (var.isType() != boundVar.isFirst()) throw TypeDBException.of(CONTRADICTORY_BOUND_VARIABLE, var);
                else if (var.isType()) {
                    Optional<LabelConstraint> existingLabel = var.asType().label();
                    if (existingLabel.isPresent() && !existingLabel.get().properLabel().equals(boundVar.first())) {
                        this.setAnswerable(false);
                    } else if (!existingLabel.isPresent()) {
                        var.asType().label(boundVar.first());
                        var.asType().setInferredTypes(set(boundVar.first()));
                    }
                } else if (var.isThing()) {
                    Optional<IIDConstraint> existingIID = var.asThing().iid();
                    if (existingIID.isPresent() && !existingIID.get().iid().equals(boundVar.second())) {
                        this.setAnswerable(false);
                    } else {
                        var.asThing().iid(boundVar.second());
                    }
                } else if (var.isValue()) {
                    assert var.asValue().assignment() == null;
                    VertexIID.Value<?> iid = VertexIID.Value.of(boundVar.second());
                    Encoding.ValueType<?> vt = iid.valueType();
                    if (vt == BOOLEAN) var.asValue().assign(new Constant.Boolean(iid.asBoolean().value()));
                    else if (vt == LONG) var.asValue().assign(new Constant.Long(iid.asLong().value()));
                    else if (vt == DOUBLE) var.asValue().assign(new Constant.Double(iid.asDouble().value()));
                    else if (vt == STRING) var.asValue().assign(new Constant.String(iid.asString().value()));
                    else if (vt == DATETIME) var.asValue().assign(new Constant.DateTime(iid.asDateTime().value()));
                    else throw TypeDBException.of(ILLEGAL_STATE);
                } else throw TypeDBException.of(ILLEGAL_STATE);
            }
        });
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

    public List<Negation> negations() {
        return negations;
    }

    public GraphTraversal.Thing traversal() {
        return traversal(Modifiers.Filter.create(list()));
    }

    public GraphTraversal.Thing traversal(Modifiers.Filter filter) {
        return traversal(filter, Modifiers.Sorting.EMPTY);
    }

    public GraphTraversal.Thing traversal(Modifiers.Filter filter, Modifiers.Sorting sorting) {
        GraphTraversal.Thing traversal = new GraphTraversal.Thing();
        variableSet.forEach(variable -> variable.addTo(traversal));
        Modifiers.Filter traversalFilter;
        if (filter.variables().isEmpty()) {
            traversalFilter = Modifiers.Filter.create(iterate(variableSet).filter(v -> v.id().isRetrievable()).map(v -> v.id().asRetrievable()).toSet());
        } else {
            assert iterate(filter.variables()).allMatch(variableMap::containsKey);
            traversalFilter = filter;
        }
        traversal.filter(traversalFilter);
        assert iterate(sorting.variables()).allMatch(variableMap::containsKey);
        traversal.sort(sorting);
        return traversal;
    }

    public void setCoherent(boolean isCoherent) {
        this.isCoherent = isCoherent;
    }

    public boolean isCoherent() {
        return isCoherent && iterate(negations).allMatch(Negation::isCoherent);
    }

    public void setAnswerable(boolean isAnswerable) {
        this.isAnswerable = isAnswerable;
    }

    public boolean isAnswerable() {
        return isAnswerable;
    }

    @Override
    public Conjunction clone() {
        return new Conjunction(VariableCloner.cloneFromConjunction(this).variables(),
                iterate(this.negations).map(Negation::clone).toList());
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
        // TODO: negations should be a set not list
        // TODO: both are corrected with https://github.com/vaticle/typedb/issues/6115
        return (this.variableSet.equals(that.variables()) &&
                this.negations.equals(that.negations()));
    }

    @Override
    public int hashCode() {
        return hash;
    }

    public static class ConstraintCloner {

        private final Map<Identifier.Variable, Variable> variables;
        private final Map<Constraint, Constraint> constraints;

        public ConstraintCloner() {
            variables = new HashMap<>();
            constraints = new HashMap<>();
        }

        public static ConstraintCloner cloneExactly(Set<? extends Constraint> s1, Constraint... s2) {
            LinkedHashSet<Constraint> ordered = new LinkedHashSet<>(s1);
            Collections.addAll(ordered, s2);
            return cloneExactly(ordered);
        }

        public static ConstraintCloner cloneExactly(Set<? extends Constraint> s1, Set<? extends Constraint> s2, Constraint... s3) {
            LinkedHashSet<Constraint> ordered = new LinkedHashSet<>(s1);
            ordered.addAll(s2);
            Collections.addAll(ordered, s3);
            return cloneExactly(ordered);
        }

        public static ConstraintCloner cloneExactly(Constraint constraint) {
            LinkedHashSet<Constraint> orderedSet = new LinkedHashSet<>();
            orderedSet.add(constraint);
            return cloneExactly(orderedSet);
        }

        private static ConstraintCloner cloneExactly(LinkedHashSet<? extends Constraint> constraints) {
            ConstraintCloner cloner = new ConstraintCloner();
            constraints.forEach(cloner::clone);
            return cloner;
        }

        private void clone(Constraint constraint) {
            constraints.put(constraint, constraint.clone(this));
        }

        public ThingVariable cloneVariable(ThingVariable variable) {
            return variables.computeIfAbsent(variable.id(), identifier -> {
                ThingVariable clone = new ThingVariable(identifier);
                clone.addInferredTypes(variable.inferredTypes());
                return clone;
            }).asThing();
        }

        public TypeVariable cloneVariable(TypeVariable variable) {
            return variables.computeIfAbsent(variable.id(), identifier -> {
                TypeVariable clone = new TypeVariable(identifier);
                clone.addInferredTypes(variable.inferredTypes());
                return clone;
            }).asType();
        }

        public ValueVariable cloneVariable(ValueVariable variable) {
            return variables.computeIfAbsent(variable.id(), ValueVariable::new).asValue();
        }

        public Conjunction conjunction() {
            return new Conjunction(set(variables.values()), list());
        }

        public Constraint getClone(Constraint constraint) {
            return constraints.get(constraint);
        }
    }
}
