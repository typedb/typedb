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

package grakn.core.query.pattern.variable;

import grakn.core.common.exception.GraknException;
import graql.lang.pattern.variable.Reference;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Query.ANONYMOUS_TYPE_VARIABLE;

public class VariableRegistry {

    private final Map<Reference, TypeVariable> types;
    private final Map<Reference, ThingVariable> things;
    private final Set<ThingVariable> anonymous;

    public VariableRegistry() {
        types = new HashMap<>();
        things = new HashMap<>();
        anonymous = new HashSet<>();
    }

    public Variable register(final graql.lang.pattern.variable.BoundVariable graqlVar) {
        if (graqlVar.isThing()) return register(graqlVar.asThing());
        else if (graqlVar.isType()) return register(graqlVar.asType());
        else throw GraknException.of(ILLEGAL_STATE);
    }

    public TypeVariable register(final graql.lang.pattern.variable.TypeVariable graqlVar) {
        if (graqlVar.reference().isAnonymous()) throw GraknException.of(ANONYMOUS_TYPE_VARIABLE);
        return types.computeIfAbsent(
                graqlVar.reference(), ref -> new TypeVariable(Identifier.of(ref.asReferrable()))
        ).constrain(graqlVar.constraints(), this);
    }

    public ThingVariable register(final graql.lang.pattern.variable.ThingVariable<?> graqlVar) {
        final ThingVariable graknVar;
        if (graqlVar.reference().isAnonymous()) {
            graknVar = new ThingVariable(Identifier.of(graqlVar.reference().asAnonymous(), anonymous.size()));
            anonymous.add(graknVar);
        } else {
            graknVar = things.computeIfAbsent(graqlVar.reference(), r -> new ThingVariable(Identifier.of(r.asReferrable())));
        }
        graknVar.constrain(graqlVar.constraints(), this);
        return graknVar;
    }

    public Set<TypeVariable> types() {
        return set(types.values());
    }

    public Set<ThingVariable> things() {
        return set(things.values(), anonymous);
    }

    public boolean contains(final Reference reference) {
        return things.containsKey(reference) || types.containsKey(reference);
    }

    public Variable get(final Reference reference) {
        if (things.containsKey(reference)) return things.get(reference);
        else return types.get(reference);
    }

    public Variable put(final Reference reference, final Variable variable) {
        if (variable.isType()) {
            things.remove(reference);
            return types.put(reference, variable.asType());
        } else if (variable.isThing()) {
            types.remove(reference);
            return things.put(reference, variable.asThing());
        } else throw GraknException.of(ILLEGAL_STATE);
    }

    public TypeVariable computeTypeIfAbsent(final Reference reference, final Function<Reference, TypeVariable> constructor) {
        if (things.containsKey(reference)) throw GraknException.of(ILLEGAL_STATE);
        return types.computeIfAbsent(reference, constructor);
    }

    public ThingVariable computeThingIfAbsent(final Reference reference, final Function<Reference, ThingVariable> constructor) {
        if (types.containsKey(reference)) throw GraknException.of(ILLEGAL_STATE);
        return things.computeIfAbsent(reference, constructor);
    }
}
