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

package grakn.core.pattern.variable;

import grabl.tracing.client.GrablTracingThreadStatic;
import grakn.core.common.exception.GraknException;
import grakn.core.traversal.common.Identifier;
import graql.lang.pattern.variable.BoundVariable;
import graql.lang.pattern.variable.ConceptVariable;
import graql.lang.pattern.variable.Reference;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;
import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.ANONYMOUS_CONCEPT_VARIABLE;
import static grakn.core.common.exception.ErrorMessage.Pattern.ANONYMOUS_TYPE_VARIABLE;
import static grakn.core.common.exception.ErrorMessage.Pattern.UNBOUNDED_CONCEPT_VARIABLE;
import static grakn.core.common.exception.ErrorMessage.Pattern.VARIABLE_CONTRADICTION;
import static java.util.Collections.unmodifiableSet;

public class VariableRegistry {

    private static final String TRACE_PREFIX = "variableregistry.";

    private final VariableRegistry bounds;
    private final boolean allowDerived;
    private final Map<Reference, TypeVariable> types;
    private final Map<Reference, ThingVariable> things;
    private final Set<ThingVariable> anonymous;

    private VariableRegistry(@Nullable VariableRegistry bounds) {
        this(bounds, true);
    }

    private VariableRegistry(@Nullable VariableRegistry bounds, boolean allowDerived) {
        this.bounds = bounds;
        this.allowDerived = allowDerived;
        types = new HashMap<>();
        things = new HashMap<>();
        anonymous = new HashSet<>();
    }

    public static VariableRegistry createFromTypes(List<graql.lang.pattern.variable.TypeVariable> variables) {
        try (GrablTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "types")) {
            VariableRegistry registry = new VariableRegistry(null);
            variables.forEach(registry::register);
            return registry;
        }
    }

    public static VariableRegistry createFromThings(List<graql.lang.pattern.variable.ThingVariable<?>> variables) {
        return createFromThings(variables, true);
    }

    public static VariableRegistry createFromThings(List<graql.lang.pattern.variable.ThingVariable<?>> variables, boolean allowDerived) {
        try (GrablTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "things")) {
            return createFromVariables(variables, null, allowDerived);
        }
    }

    public static VariableRegistry createFromVariables(List<? extends BoundVariable> variables,
                                                       @Nullable VariableRegistry bounds) {
        return createFromVariables(variables, bounds, true);
    }

    public static VariableRegistry createFromVariables(List<? extends BoundVariable> variables,
                                                       @Nullable VariableRegistry bounds, boolean allowDerived) {
        try (GrablTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "variables")) {
            List<ConceptVariable> unboundedVariables = new ArrayList<>();
            VariableRegistry registry = new VariableRegistry(bounds, allowDerived);
            variables.forEach(graqlVar -> {
                if (graqlVar.isConcept()) unboundedVariables.add(graqlVar.asConcept());
                else registry.register(graqlVar);
            });
            unboundedVariables.forEach(registry::register);
            return registry;
        }
    }

    private Variable register(graql.lang.pattern.variable.BoundVariable graqlVar) {
        if (graqlVar.isThing()) return register(graqlVar.asThing());
        else if (graqlVar.isType()) return register(graqlVar.asType());
        else if (graqlVar.isConcept()) return register(graqlVar.asConcept());
        else throw GraknException.of(ILLEGAL_STATE);
    }

    public Variable register(ConceptVariable graqlVar) {
        if (graqlVar.reference().isAnonymous()) throw GraknException.of(ANONYMOUS_CONCEPT_VARIABLE);
        if (things.containsKey(graqlVar.reference())) {
            return things.get(graqlVar.reference()).constrainConcept(graqlVar.constraints(), this);
        } else if (types.containsKey(graqlVar.reference())) {
            return types.get(graqlVar.reference()).constrainConcept(graqlVar.constraints(), this);
        } else if (bounds != null && bounds.contains(graqlVar.reference())) {
            Reference.Referable ref = graqlVar.reference().asReferable();
            if (bounds.get(graqlVar.reference()).isThing()) {
                things.put(ref, new ThingVariable(Identifier.Variable.of(ref)));
                return things.get(ref).constrainConcept(graqlVar.constraints(), this);
            } else {
                types.put(ref, new TypeVariable(Identifier.Variable.of(ref)));
                return types.get(ref).constrainConcept(graqlVar.constraints(), this);
            }
        } else {
            throw GraknException.of(UNBOUNDED_CONCEPT_VARIABLE, graqlVar.reference());
        }
    }

    public TypeVariable register(graql.lang.pattern.variable.TypeVariable graqlVar) {
        if (graqlVar.reference().isAnonymous()) throw GraknException.of(ANONYMOUS_TYPE_VARIABLE);
        return computeTypeIfAbsent(
                graqlVar.reference(), ref -> new TypeVariable(Identifier.Variable.of(ref.asReferable()))
        ).constrainType(graqlVar.constraints(), this);
    }

    public ThingVariable register(graql.lang.pattern.variable.ThingVariable<?> graqlVar) {
        ThingVariable graknVar;
        if (graqlVar.reference().isAnonymous()) {
            graknVar = new ThingVariable(Identifier.Variable.of(graqlVar.reference().asAnonymous(), anonymous.size()));
            anonymous.add(graknVar);
        } else {
            graknVar = computeThingIfAbsent(graqlVar.reference(), r -> new ThingVariable(Identifier.Variable.of(r.asReferable())));
        }
        return graknVar.constrainThing(graqlVar.constraints(), this);
    }

    public boolean allowsDerived() {
        return allowDerived;
    }

    public Set<TypeVariable> types() {
        return set(types.values());
    }

    public Set<ThingVariable> things() {
        return set(things.values(), anonymous);
    }

    public Set<Variable> variables() {
        Set<Variable> output = new HashSet<>();
        output.addAll(types.values());
        output.addAll(things.values());
        output.addAll(anonymous);
        return unmodifiableSet(output);
    }

    public boolean contains(Reference reference) {
        return things.containsKey(reference) || types.containsKey(reference);
    }

    public Variable get(Reference reference) {
        if (things.containsKey(reference)) return things.get(reference);
        else return types.get(reference);
    }

    public TypeVariable computeTypeIfAbsent(Reference reference, Function<Reference, TypeVariable> constructor) {
        if (things.containsKey(reference)) {
            throw GraknException.of(VARIABLE_CONTRADICTION, reference);
        } else return types.computeIfAbsent(reference, constructor);
    }

    public ThingVariable computeThingIfAbsent(Reference reference, Function<Reference, ThingVariable> constructor) {
        if (types.containsKey(reference)) throw GraknException.of(VARIABLE_CONTRADICTION, reference);
        else return things.computeIfAbsent(reference, constructor);
    }
}
