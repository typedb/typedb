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
import grakn.core.concept.Concept;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.traversal.common.Identifier;
import grakn.core.traversal.common.Predicate;
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

    private final Map<Identifier, Set<Identifier>> unifier;
    private final Map<Identifier, Set<Identifier>> reverseUnifier;
    private final Requirements requirements;

    private Unifier(Map<Identifier, Set<Identifier>> unifier, Requirements requirements) {
        this.unifier = Collections.unmodifiableMap(unifier);
        this.requirements = requirements;
        this.reverseUnifier = reverse(this.unifier);
    }

    public ConceptMap unify(ConceptMap toUnify) {
        Map<Reference.Name, Concept> unified = new HashMap<>();

        toUnify.concepts().forEach((ref, concept) -> {
            Identifier.Variable asIdentifier = Identifier.Variable.of(ref);
            assert unifier.containsKey(asIdentifier);
            Set<Identifier> unifiedIdentifiers = unifier.get(asIdentifier);

            for (Identifier unifiedIdentifier : unifiedIdentifiers) {
                if (unifiedIdentifier.isNamedReference()) {
                    Reference.Name unifiedReference = unifiedIdentifier.asVariable().reference().asName();
                    assert !unified.containsKey(unifiedReference);
                    unified.put(unifiedReference, concept);
                }
            }
        });

//        return Optional.empty(); // TODO ... why does this have to be optional? Can it fail?
        return new ConceptMap(unified);
    }

    /**
     * Un-unify a map of concepts, with given identifiers. These must include anonymous and labelled concepts,
     * as they may be mapped to from a named variable, and may have requirements that need to be met.
     */
    public Optional<ConceptMap> unUnify(Map<Identifier, Concept> identifiedConcepts) {

        /*
        1. apply the reverse unifier - if a mapping failure is found (eg. x,y should both become a but are not equal)
           then return empty optional
        2. confirm that each unified concept in the map meets the its requirements
        3. filter down the identified map to a concept map of just named identifiers and their concepts
         */

        return Optional.empty(); // TODO
    }

    // visible for testing
    public Requirements requirements() {
        return requirements;
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

    public Map<Identifier, Set<Identifier>> mapping() {
        return unifier;
    }

    public static Unifier.Builder builder() {
        return new Builder();
    }

    private Map<Identifier, Set<Identifier>> reverse(Map<Identifier, Set<Identifier>> unifier) {
        Map<Identifier, Set<Identifier>> reverse = new HashMap<>();
        unifier.forEach((unify, unifieds) -> {
            for (Identifier unified : unifieds) {
                reverse.putIfAbsent(unified, new HashSet<>());
                reverse.get(unified).add(unify);
            }
        });
        return reverse;
    }

    public static class Builder {

        Map<Identifier, Set<Identifier>> unifier;
        Requirements requirements;

        public Builder() {
            this(new HashMap<>(), new Requirements());
        }

        private Builder(Map<Identifier, Set<Identifier>> unifier, Requirements requirements) {
            this.unifier = unifier;
            this.requirements = requirements;
        }

        public void add(Identifier source, Identifier target) {
            unifier.putIfAbsent(source, new HashSet<>());
            unifier.get(source).add(target);
        }

        public Requirements requirements() {
            return requirements;
        }

        public Unifier build() {
            return new Unifier(unifier, requirements);
        }

        public Builder duplicate() {
            Map<Identifier, Set<Identifier>> unifierCopy = new HashMap<>();
            unifier.forEach(((identifier, unifieds) -> unifierCopy.put(identifier, set(unifieds))));
            Requirements requirementsCopy = requirements.duplicate();
            return new Builder(unifierCopy, requirementsCopy);
        }
    }

    /*
    Record requirements that may be used to fail a unification

    Allowed requirements we may impose:
    1. a type variable may be required to within a set of allowed types
    2. a thing variable may be required to be an explicit instance of a set of allowed types
    3. a thing variable may have to satisfy a specific predicate (ie when it's an attribute)

    Note that in the future we may treat these (and variable equality constraints encoded in unifier)
     as constraints we can use to rewrite queries that are unified with. This would be more efficient,
     but query rewriting was designed _out_ of our architecture, and will have to be carefully re-added.
     */
    public static class Requirements {
        Map<Identifier, Set<Label>> types;
        Map<Identifier, Set<Label>> isaExplicit;
        Map<Identifier, Set<Predicate<?, ?>>> predicates;

        public Requirements() {
            this(new HashMap<>(), new HashMap<>(), new HashMap<>());
        }

        public Requirements(Map<Identifier, Set<Label>> types, Map<Identifier, Set<Label>> isaExplicit,
                            Map<Identifier, Set<Predicate<?, ?>>> predicates) {
            this.types = types;
            this.isaExplicit = isaExplicit;
            this.predicates = predicates;
        }

        public void types(Identifier identifier, Set<Label> labels) {
            assert !types.containsKey(identifier);
            types.put(identifier, set(labels));
        }

        public void isaExplicit(Identifier identifier, Set<Label> labels) {
            assert !isaExplicit.containsKey(identifier);
            isaExplicit.put(identifier, set(labels));
        }

        public void predicates(Identifier identifier, Set<Predicate<?, ?>> preds) {
            assert !predicates.containsKey(identifier);
            predicates.put(identifier, preds);
        }

        public Map<Identifier, Set<Label>> types() { return types; }

        public Map<Identifier, Set<Label>> isaExplicit() { return isaExplicit; }

        public Map<Identifier, Set<Predicate<?, ?>>> predicates() { return predicates; }

        private Requirements duplicate() {
            Map<Identifier, Set<Label>> typesCopy = new HashMap<>();
            Map<Identifier, Set<Label>> isaExplicitCopy = new HashMap<>();
            Map<Identifier, Set<Predicate<?, ?>>> predicatesCopy = new HashMap<>();
            types.forEach(((identifier, labels) -> typesCopy.put(identifier, set(labels))));
            isaExplicit.forEach(((identifier, labels) -> isaExplicitCopy.put(identifier, set(labels))));
            predicates.forEach(((identifier, preds) -> predicatesCopy.put(identifier, set(preds))));
            return new Requirements(typesCopy, isaExplicitCopy, predicatesCopy);
        }
    }
}
