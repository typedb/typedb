/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.pattern.variable;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.common.Reference;
import com.vaticle.typeql.lang.common.TypeQLVariable;
import com.vaticle.typeql.lang.pattern.statement.ConceptStatement;
import com.vaticle.typeql.lang.pattern.statement.Statement;
import com.vaticle.typeql.lang.pattern.statement.ThingStatement;
import com.vaticle.typeql.lang.pattern.statement.TypeStatement;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.concatToSet;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.ANONYMOUS_CONCEPT_VARIABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.ANONYMOUS_TYPE_VARIABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.UNBOUNDED_CONCEPT_VARIABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.VARIABLE_CONTRADICTION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.VARIABLE_NAME_CONFLICT;
import static java.util.Collections.unmodifiableSet;

public class VariableRegistry {

    private final VariableRegistry bounds;
    private final boolean allowDerived;
    private final Map<Reference, TypeVariable> types;
    private final Map<Reference, ThingVariable> things;
    private final Map<Reference.Name.Value, ValueVariable> values;
    private final Set<ThingVariable> anonymous;
    private final int reservedAnonymous;

    private VariableRegistry(@Nullable VariableRegistry bounds) {
        this(bounds, true);
    }

    private VariableRegistry(@Nullable VariableRegistry bounds, boolean allowDerived) {
        this(bounds, allowDerived, 0);
    }

    private VariableRegistry(@Nullable VariableRegistry bounds, boolean allowDerived, int reservedAnonymous) {
        this.bounds = bounds;
        this.allowDerived = allowDerived;
        types = new HashMap<>();
        things = new HashMap<>();
        values = new HashMap<>();
        anonymous = new HashSet<>();
        this.reservedAnonymous = reservedAnonymous;
    }

    /**
     * This is a workaround to prevent clashes between unrelated patterns that should not share anonymous variables
     */
    public static VariableRegistry createReservedAnonymous(int reservedAnonymous) {
        return new VariableRegistry(null, true, reservedAnonymous);
    }

    public static VariableRegistry createFromTypes(List<TypeStatement> statements) {
        VariableRegistry registry = new VariableRegistry(null);
        statements.forEach(registry::registerStatement);
        return registry;
    }

    public static VariableRegistry createFromThings(List<ThingStatement<?>> statements) {
        return createFromThings(statements, true);
    }

    public static VariableRegistry createFromThings(List<ThingStatement<?>> statements, boolean allowDerived) {
        return createFromStatements(statements, null, allowDerived);
    }

    public static VariableRegistry createFromStatements(List<? extends Statement> statements,
                                                        @Nullable VariableRegistry bounds) {
        return createFromStatements(statements, bounds, true);
    }

    public static VariableRegistry createFromStatements(List<? extends Statement> statements,
                                                        @Nullable VariableRegistry bounds, boolean allowDerived) {
        List<ConceptStatement> unboundedVariables = new ArrayList<>();
        VariableRegistry registry = new VariableRegistry(bounds, allowDerived);
        statements.forEach(statement -> {
            if (statement.isThing()) registry.registerStatement(statement.asThing());
            else if (statement.isType()) registry.registerStatement(statement.asType());
            else if (statement.isValue()) registry.registerStatement(statement.asValue());
            else if (statement.isConcept()) unboundedVariables.add(statement.asConcept());
            else throw TypeDBException.of(ILLEGAL_STATE);
        });
        unboundedVariables.forEach(registry::registerStatement);
        return registry;
    }

    public Variable registerStatement(com.vaticle.typeql.lang.pattern.statement.ConceptStatement typeQLStatement) {
        Reference reference = typeQLStatement.headVariable().reference();
        if (reference.isAnonymous()) throw TypeDBException.of(ANONYMOUS_CONCEPT_VARIABLE);
        if (things.containsKey(reference)) {
            return things.get(reference).constrainConcept(typeQLStatement.constraints(), this);
        } else if (types.containsKey(reference)) {
            return types.get(reference).constrainConcept(typeQLStatement.constraints(), this);
        } else if (bounds != null && bounds.isRegistered(reference)) {
            assert reference.isName();
            Reference.Name ref = reference.asName();
            Variable variable = bounds.get(reference);
            if (variable.isThing()) {
                things.put(ref, new ThingVariable(Identifier.Variable.of(ref)));
                return things.get(ref).constrainConcept(typeQLStatement.constraints(), this);
            } else if (variable.isType()) {
                types.put(ref, new TypeVariable(Identifier.Variable.of(ref)));
                return types.get(ref).constrainConcept(typeQLStatement.constraints(), this);
            } else throw TypeDBException.of(ILLEGAL_STATE);
        } else {
            throw TypeDBException.of(UNBOUNDED_CONCEPT_VARIABLE, reference);
        }
    }

    public TypeVariable registerStatement(com.vaticle.typeql.lang.pattern.statement.TypeStatement typeQLStatement) {
        return registerTypeVariable(typeQLStatement.headVariable())
                .constrainType(typeQLStatement.constraints(), this);
    }

    public TypeVariable registerTypeVariable(TypeQLVariable.Concept var) {
        if (var.reference().isAnonymous()) throw TypeDBException.of(ANONYMOUS_TYPE_VARIABLE);
        Reference reference = var.reference();
        if (things.containsKey(reference)) {
            throw TypeDBException.of(VARIABLE_CONTRADICTION, reference);
        } else if (reference.isName() && values.containsKey(Reference.value(reference.name()))) {
            throw TypeDBException.of(VARIABLE_NAME_CONFLICT, reference.name());
        } else return types.computeIfAbsent(reference, ref -> {
            if (var.reference().isName()) return new TypeVariable(Identifier.Variable.of(ref.asName()));
            else if (var.reference().isLabel()) {
                TypeVariable typeVariable = new TypeVariable(Identifier.Variable.of(ref.asLabel()));
                typeVariable.label(Label.of(ref.asLabel().label(), ref.asLabel().scope().orElse(null)));
                return typeVariable;
            }
            else throw TypeDBException.of(ILLEGAL_STATE);
        });
    }

    public ThingVariable registerStatement(ThingStatement<?> typeQLStatement) {
        return registerThingVariable(typeQLStatement.headVariable())
                .constrainThing(typeQLStatement.constraints(), this);
    }

    public ThingVariable registerThingVariable(TypeQLVariable.Concept var) {
        ThingVariable thingVar;
        if (var.reference().isAnonymous()) {
            thingVar = new ThingVariable(Identifier.Variable.of(var.reference().asAnonymous(), anonymousCounter()));
            anonymous.add(thingVar);
        } else {
            Reference reference = var.reference();
            if (types.containsKey(reference)) throw TypeDBException.of(VARIABLE_CONTRADICTION, reference);
            else if (reference.isName() && values.containsKey(Reference.value(reference.name()))) {
                throw TypeDBException.of(VARIABLE_NAME_CONFLICT, reference.name());
            } else {
                thingVar = things.computeIfAbsent(reference, r -> new ThingVariable(Identifier.Variable.of(r.asName())));
            }
        }
        return thingVar;
    }

    public ValueVariable registerStatement(com.vaticle.typeql.lang.pattern.statement.ValueStatement typeQLStatement) {
        return registerValueVariable(typeQLStatement.headVariable())
                .constrainValue(typeQLStatement.constraints(), this);
    }

    public ValueVariable registerValueVariable(TypeQLVariable.Value var) {
        Reference.Name.Value reference = var.reference().asName().asValue();
        Reference asConceptReference = Reference.concept(reference.name());
        if (things.containsKey(asConceptReference) || types.containsKey(asConceptReference)) {
            throw TypeDBException.of(VARIABLE_NAME_CONFLICT, reference.name());
        } else {
            return values.computeIfAbsent(reference, r -> new ValueVariable(Identifier.Variable.of(r.asName())));
        }
    }

    public int anonymousCounter() {
        if (bounds != null) return bounds.anonymousCounter() + anonymous.size() + reservedAnonymous;
        else return anonymous.size() + reservedAnonymous;
    }

    public boolean allowsDerived() {
        return allowDerived;
    }

    public Set<TypeVariable> types() {
        return set(types.values());
    }

    public Set<ThingVariable> things() {
        return concatToSet(things.values(), anonymous);
    }

    public Set<ValueVariable> values() {
        return set(values.values());
    }

    public Set<Variable> variables() {
        Set<Variable> output = new HashSet<>();
        output.addAll(types.values());
        output.addAll(things.values());
        output.addAll(values.values());
        output.addAll(anonymous);
        return unmodifiableSet(output);
    }

    public Optional<VariableRegistry> bounds() {
        return Optional.ofNullable(bounds);
    }

    public boolean isRegistered(Reference reference) {
        return things.containsKey(reference) || types.containsKey(reference) || values.containsKey(reference);
    }

    public boolean isBound(Reference reference) {
        return isRegistered(reference) || bounds().map(b -> b.isBound(reference)).orElse(false);
    }

    public Variable get(Reference reference) {
        if (things.containsKey(reference)) return things.get(reference);
        else if (types.containsKey(reference)) return types.get(reference);
        else return values.get(reference);
    }

}
