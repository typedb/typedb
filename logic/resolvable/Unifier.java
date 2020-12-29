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

package grakn.core.logic.resolvable;

import grakn.core.common.exception.GraknException;
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
import java.util.stream.Collectors;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Reasoner.UN_UNIFICATION_MISSING_CONCEPT;

public class Unifier {

    private final Map<Identifier, Set<Identifier>> unifier;
    private final Map<Identifier, Set<Identifier>> unUnifier;
    private final Requirements requirements;

    private Unifier(Map<Identifier, Set<Identifier>> unifier, Requirements requirements) {
        this.unifier = Collections.unmodifiableMap(unifier);
        this.requirements = requirements;
        this.unUnifier = reverse(this.unifier);
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

        // TODO yes this can fail, as we could try to forward unify a mapping x -> a, y -> a, where x and y are already known
//        return Optional.empty(); // TODO ... why does this have to be optional? Can it fail?
        return new ConceptMap(unified);
    }

    /**
     * Un-unify a map of concepts, with given identifiers. These must include anonymous and labelled concepts,
     * as they may be mapped to from a named variable, and may have requirements that need to be met.
     */
    public Optional<ConceptMap> unUnify(Map<Identifier, Concept> identifiedConcepts) {
        Map<Identifier, Concept> reversedConcepts = new HashMap<>();

        for (Map.Entry<Identifier, Set<Identifier>> entry : unUnifier.entrySet()) {
            Identifier toReverse = entry.getKey();
            Set<Identifier> reversed = entry.getValue();
            if (!identifiedConcepts.containsKey(toReverse)) {
                throw GraknException.of(UN_UNIFICATION_MISSING_CONCEPT, toReverse, identifiedConcepts);
            }
            Concept concept = identifiedConcepts.get(toReverse);
            for (Identifier r : reversed) {
                if (!reversedConcepts.containsKey(r)) reversedConcepts.put(r, concept);
                if (!reversedConcepts.get(r).equals(concept)) return Optional.empty();
            }
        }

        // TODO implement Requirement satisfaction

        return Optional.of(conceptMap(reversedConcepts));
    }

    Requirements requirements() {
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
    public String toString() {
        return "Unifier{" +
                "unifier=" + unifierString(unifier) +
                ", unUnifier=" + unifierString(unUnifier) +
                ", requirements=" + requirements +
                '}';
    }

    private String unifierString(Map<Identifier, Set<Identifier>> unifier) {
        return "{" + unifier.entrySet().stream().map(e -> e.getKey() + "->" + e.getValue()).collect(Collectors.joining(",")) + "}";
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

    private ConceptMap conceptMap(Map<Identifier, Concept> identifiedConcepts) {
        Map<Reference.Name, Concept> namedConcepts = new HashMap<>();
        for (Map.Entry<Identifier, Concept> identifiedConcept : identifiedConcepts.entrySet()) {
            if (identifiedConcept.getKey().isNamedReference()) {
                assert identifiedConcept.getKey().asVariable().reference().isName();
                namedConcepts.put(identifiedConcept.getKey().asVariable().reference().asName(), identifiedConcept.getValue());
            }
        }
        return new ConceptMap(namedConcepts);
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
            assert !types.containsKey(identifier) || types.get(identifier).equals(labels);
            types.put(identifier, set(labels));
        }

        public void isaExplicit(Identifier identifier, Set<Label> labels) {
            assert !isaExplicit.containsKey(identifier) || isaExplicit.get(identifier).equals(labels);
            isaExplicit.put(identifier, set(labels));
        }

        public void predicates(Identifier identifier, Set<Predicate<?, ?>> preds) {
            assert !predicates.containsKey(identifier) || predicates.get(identifier).equals(preds);
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
