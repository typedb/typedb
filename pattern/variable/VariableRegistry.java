/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.pattern.variable;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.pattern.variable.BoundVariable;
import com.vaticle.typeql.lang.pattern.variable.ConceptVariable;
import com.vaticle.typeql.lang.pattern.variable.Reference;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

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

    public static VariableRegistry createFromTypes(List<com.vaticle.typeql.lang.pattern.variable.TypeVariable> variables) {
        VariableRegistry registry = new VariableRegistry(null);
        variables.forEach(registry::register);
        return registry;
    }

    public static VariableRegistry createFromThings(List<com.vaticle.typeql.lang.pattern.variable.ThingVariable<?>> variables) {
        return createFromThings(variables, true);
    }

    public static VariableRegistry createFromThings(List<com.vaticle.typeql.lang.pattern.variable.ThingVariable<?>> variables, boolean allowDerived) {
        return createFromVariables(variables, null, allowDerived);
    }

    public static VariableRegistry createFromVariables(List<? extends BoundVariable> variables,
                                                       @Nullable VariableRegistry bounds) {
        return createFromVariables(variables, bounds, true);
    }

    public static VariableRegistry createFromVariables(List<? extends BoundVariable> variables,
                                                       @Nullable VariableRegistry bounds, boolean allowDerived) {
        List<ConceptVariable> unboundedVariables = new ArrayList<>();
        VariableRegistry registry = new VariableRegistry(bounds, allowDerived);
        variables.forEach(typeQLVar -> {
            if (typeQLVar.isThing()) registry.register(typeQLVar.asThing());
            else if (typeQLVar.isType()) registry.register(typeQLVar.asType());
            else if (typeQLVar.isValue()) registry.register(typeQLVar.asValue());
            else if (typeQLVar.isConcept()) unboundedVariables.add(typeQLVar.asConcept());
            else throw TypeDBException.of(ILLEGAL_STATE);
        });
        unboundedVariables.forEach(registry::register);
        return registry;
    }

    public Variable register(com.vaticle.typeql.lang.pattern.variable.ConceptVariable typeQLVar) {
        if (typeQLVar.reference().isAnonymous()) throw TypeDBException.of(ANONYMOUS_CONCEPT_VARIABLE);
        if (things.containsKey(typeQLVar.reference())) {
            return things.get(typeQLVar.reference()).constrainConcept(typeQLVar.constraints(), this);
        } else if (types.containsKey(typeQLVar.reference())) {
            return types.get(typeQLVar.reference()).constrainConcept(typeQLVar.constraints(), this);
        } else if (bounds != null && bounds.isRegistered(typeQLVar.reference())) {
            assert typeQLVar.reference().isName();
            Reference.Name ref = typeQLVar.reference().asName();
            if (bounds.get(typeQLVar.reference()).isThing()) {
                things.put(ref, new ThingVariable(Identifier.Variable.of(ref)));
                return things.get(ref).constrainConcept(typeQLVar.constraints(), this);
            } else if (bounds.get(typeQLVar.reference()).isType()) {
                types.put(ref, new TypeVariable(Identifier.Variable.of(ref)));
                return types.get(ref).constrainConcept(typeQLVar.constraints(), this);
            } else throw TypeDBException.of(ILLEGAL_STATE);
        } else {
            throw TypeDBException.of(UNBOUNDED_CONCEPT_VARIABLE, typeQLVar.reference());
        }
    }

    public TypeVariable register(com.vaticle.typeql.lang.pattern.variable.TypeVariable typeQLVar) {
        if (typeQLVar.reference().isAnonymous()) throw TypeDBException.of(ANONYMOUS_TYPE_VARIABLE);
        return computeTypeIfAbsent(
                typeQLVar.reference(), ref -> {
                    if (typeQLVar.reference().isName()) return new TypeVariable(Identifier.Variable.of(ref.asName()));
                    else if (typeQLVar.reference().isLabel())
                        return new TypeVariable(Identifier.Variable.of(ref.asLabel()));
                    else throw TypeDBException.of(ILLEGAL_STATE);
                }
        ).constrainType(typeQLVar.constraints(), this);
    }

    public ThingVariable register(com.vaticle.typeql.lang.pattern.variable.ThingVariable<?> typeQLVar) {
        ThingVariable typeDBVar;
        if (typeQLVar.reference().isAnonymous()) {
            typeDBVar = new ThingVariable(Identifier.Variable.of(typeQLVar.reference().asAnonymous(), anonymousCounter()));
            anonymous.add(typeDBVar);
        } else {
            typeDBVar = computeThingIfAbsent(typeQLVar.reference(), r -> new ThingVariable(Identifier.Variable.of(r.asName())));
        }
        return typeDBVar.constrainThing(typeQLVar.constraints(), this);
    }

    public ValueVariable register(com.vaticle.typeql.lang.pattern.variable.ValueVariable typeQLVar) {
        ValueVariable typeDBVar = computeValueIfAbsent(
                typeQLVar.reference().asName().asValue(), r -> new ValueVariable(Identifier.Variable.of(r.asName()))
        );
        return typeDBVar.constrainValue(typeQLVar.constraints(), this);
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

    public TypeVariable computeTypeIfAbsent(Reference reference, Function<Reference, TypeVariable> constructor) {
        if (things.containsKey(reference)) {
            throw TypeDBException.of(VARIABLE_CONTRADICTION, reference);
        } else if (reference.isName() && values.containsKey(Reference.value(reference.name()))) {
            throw TypeDBException.of(VARIABLE_NAME_CONFLICT, reference.name());
        } else return types.computeIfAbsent(reference, constructor);
    }

    public ThingVariable computeThingIfAbsent(Reference reference, Function<Reference, ThingVariable> constructor) {
        if (types.containsKey(reference)) {
            throw TypeDBException.of(VARIABLE_CONTRADICTION, reference);
        } else if (reference.isName() && values.containsKey(Reference.value(reference.name()))) {
            throw TypeDBException.of(VARIABLE_NAME_CONFLICT, reference.name());
        } else return things.computeIfAbsent(reference, constructor);
    }

    public ValueVariable computeValueIfAbsent(Reference.Name.Value reference, Function<Reference, ValueVariable> constructor) {
        Reference asConceptReference = Reference.concept(reference.name());
        if (things.containsKey(asConceptReference) || types.containsKey(asConceptReference)) {
            throw TypeDBException.of(VARIABLE_NAME_CONFLICT, reference.name());
        } else return values.computeIfAbsent(reference, constructor);
    }
}
