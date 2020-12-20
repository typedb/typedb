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
 */

package grakn.core.logic.transformer;

import grakn.core.common.parameters.Label;
import grakn.core.concept.answer.ConceptMap;
import graql.lang.pattern.variable.Reference;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static grakn.common.collection.Collections.set;

public class Unifier extends VariableTransformer {

    private final Map<Reference.Name, Set<Reference.Name>> unifier;
    private final Map<Reference.Name, Set<Label>> allowedTypes;

    Unifier(Map<Reference.Name, Set<Reference.Name>> unifier, Map<Reference.Name, Set<Label>> allowedTypes) {
        this.unifier = Collections.unmodifiableMap(unifier);
        this.allowedTypes = Collections.unmodifiableMap(allowedTypes);
    }

    public static Unifier of(Map<Reference.Name, Set<Reference.Name>> unifier,
                             Map<Reference.Name, Set<Label>> allowedTypes) {
        return new Unifier(unifier, allowedTypes);
    }

    public static Unifier empty() {
        return new Unifier(new HashMap<>(), new HashMap<>());
    }

    public Optional<ConceptMap> unify(ConceptMap toUnify) {
        return Optional.empty(); // TODO
    }

    public Optional<ConceptMap> unUnify(ConceptMap conceptMap) {
        return Optional.empty(); // TODO
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Unifier that = (Unifier) o;
        return unifier.equals(that.unifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(unifier);
    }

    @Override
    public boolean isUnifier() {
        return true;
    }

    @Override
    public Unifier asUnifier() {
        return this;
    }

    public Map<Reference.Name, Set<Reference.Name>> mapping() {
        return unifier;
    }

    public Unifier extend(Reference.Name source, Reference.Name target, Set<Label> allowedSourceTypes) {
        Map<Reference.Name, Set<Reference.Name>> unifierClone = new HashMap<>();
        unifier.forEach((ref, unifieds) -> unifierClone.put(ref, set(unifieds)));
        unifierClone.putIfAbsent(source, new HashSet<>());
        unifierClone.get(source).add(target);
        Map<Reference.Name, Set<Label>> typesClone = new HashMap<>();
        allowedTypes.forEach((ref, types) -> typesClone.put(ref, set(types)));
        if (typesClone.containsKey(source)) assert typesClone.get(source).equals(allowedSourceTypes);
        else typesClone.put(source, set(allowedSourceTypes));
        return new Unifier(unifierClone, typesClone);
    }

    public Unifier.Builder builder() {
        return new Builder();
    }

    public class Builder {
        Map<Reference.Name, Set<Reference.Name>> unifierBuilder;
        Map<Reference.Name, Set<Label>> allowedTypesBuilder;
        public Builder() {
             this.unifierBuilder = new HashMap<>();
             this.allowedTypesBuilder = new HashMap<>();
        }

        public void add(Reference.Name source, Reference.Name target, Set<Label> types) {
            unifierBuilder.putIfAbsent(source, new HashSet<>());
            unifierBuilder.get(source).add(target);
            if (allowedTypesBuilder.containsKey(source)) assert allowedTypesBuilder.get(source).equals(types);
            else allowedTypesBuilder.put(source, new HashSet<>(types));
        }

        public Unifier build() {
            unifier.forEach((ref, unifieds) -> {
                assert !unifierBuilder.containsKey(ref);
                unifierBuilder.put(ref, set(unifieds));
            });
            allowedTypes.forEach((ref, labels) -> {
                assert !allowedTypesBuilder.containsKey(ref);
                allowedTypesBuilder.put(ref, set(labels));
            });
            return new Unifier(unifier, allowedTypesBuilder);
        }
    }
}
