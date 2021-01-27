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
 */

package grakn.core.logic.resolvable;

import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Label;
import grakn.core.concept.Concept;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Attribute;
import grakn.core.traversal.common.Identifier;
import graql.lang.pattern.variable.Reference;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Reasoner.REVERSE_UNIFICATION_MISSING_CONCEPT;

public class Unifier {

    private final Map<Identifier, Set<Identifier>> unifier;
    private final Map<Identifier, Set<Identifier>> unUnifier;
    private final Requirements requirements;

    private Unifier(Map<Identifier, Set<Identifier>> unifier, Requirements requirements) {
        this.unifier = Collections.unmodifiableMap(unifier);
        this.requirements = requirements;
        this.unUnifier = reverse(this.unifier);
    }

    public static Unifier.Builder builder() {
        return new Builder();
    }

    /*
    Returns a best-effort forward unification. It may produce an empty concept map, or a not-present Optional.
    An empty concept map means that none of the concepts were required by this unifier.
    Eg.  unifier { b -> x, c -> y}.unify({a = Attr(0x10}) -> Optional.of({})
    However, a not-present Optional may be produced:
    Eg. unifier { b -> x, c -> x}.unify({b = Attr(0x10), c = Attr(0x20)}) -> Optional.empty()

    the latter will never be valid as it is a contradiction, the former empty map is the result of the unifier's filtering
     */

    public Optional<ConceptMap> unify(ConceptMap conceptMap) {
        Map<Identifier, Concept> unifiedMap = new HashMap<>();

        for (Map.Entry<Identifier, Set<Identifier>> entry : unifier.entrySet()) {
            Identifier toUnify = entry.getKey();
            Set<Identifier> unifieds = entry.getValue();
            if (toUnify.isName() && conceptMap.contains(toUnify.asVariable().reference().asName())) {
                Concept concept = conceptMap.get(toUnify.asVariable().reference().asName());
                for (Identifier unified : unifieds) {
                    if (!unifiedMap.containsKey(unified)) unifiedMap.put(unified, concept);
                    if (!unifiedMap.get(unified).equals(concept)) return Optional.empty();
                }
            }
        }
        return Optional.of(conceptMap(unifiedMap));
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
                throw GraknException.of(REVERSE_UNIFICATION_MISSING_CONCEPT, toReverse, identifiedConcepts);
            }
            Concept concept = identifiedConcepts.get(toReverse);
            for (Identifier r : reversed) {
                if (!reversedConcepts.containsKey(r)) reversedConcepts.put(r, concept);
                if (!reversedConcepts.get(r).equals(concept)) return Optional.empty();
            }
        }

        if (requirements().satisfiedBy(reversedConcepts)) return Optional.of(conceptMap(reversedConcepts));
        else return Optional.empty();
    }

    public Map<Identifier, Set<Identifier>> mapping() {
        return unifier;
    }

    Requirements requirements() {
        return requirements;
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
            if (identifiedConcept.getKey().isName()) {
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
        Map<Identifier, Function<Attribute, Boolean>> predicates;

        public Requirements() {
            this(new HashMap<>(), new HashMap<>(), new HashMap<>());
        }

        public Requirements(Map<Identifier, Set<Label>> types, Map<Identifier, Set<Label>> isaExplicit,
                            Map<Identifier, Function<Attribute, Boolean>> predicates) {
            this.types = types;
            this.isaExplicit = isaExplicit;
            this.predicates = predicates;
        }

        public boolean satisfiedBy(Map<Identifier, Concept> concepts) {
            for (Map.Entry<Identifier, Concept> identifiedConcept : concepts.entrySet()) {
                Identifier id = identifiedConcept.getKey();
                Concept concept = identifiedConcept.getValue();
                if (!(typesSatisfied(id, concept) && isaXSatisfied(id, concept) && predicatesSatisfied(id, concept))) {
                    return false;
                }
            }
            return true;
        }

        private boolean typesSatisfied(Identifier id, Concept concept) {
            if (types.containsKey(id)) {
                assert concept.isType();
                return types.get(id).contains(concept.asType().getLabel());
            } else {
                return true;
            }
        }

        private boolean isaXSatisfied(Identifier id, Concept concept) {
            if (isaExplicit.containsKey(id)) {
                assert concept.isThing();
                return isaExplicit.get(id).contains(concept.asThing().getType().getLabel());
            } else {
                return true;
            }
        }

        private boolean predicatesSatisfied(Identifier id, Concept concept) {
            if (predicates.containsKey(id)) {
                assert concept.isThing() && (concept.asThing() instanceof Attribute);
                return predicates.get(id).apply(concept.asAttribute());
            } else {
                return true;
            }
        }

        public void types(Identifier identifier, Set<Label> labels) {
            assert !types.containsKey(identifier) || types.get(identifier).equals(labels);
            types.put(identifier, set(labels));
        }

        public void isaExplicit(Identifier identifier, Set<Label> labels) {
            assert !isaExplicit.containsKey(identifier) || isaExplicit.get(identifier).equals(labels);
            isaExplicit.put(identifier, set(labels));
        }

        public void predicates(Identifier identifier, Function<Attribute, Boolean> predicateFn) {
            assert !predicates.containsKey(identifier);
            predicates.put(identifier, predicateFn);
        }

        public Map<Identifier, Set<Label>> types() { return types; }

        public Map<Identifier, Set<Label>> isaExplicit() { return isaExplicit; }

        public Map<Identifier, Function<Attribute, Boolean>> predicates() { return predicates; }

        private Requirements duplicate() {
            Map<Identifier, Set<Label>> typesCopy = new HashMap<>();
            Map<Identifier, Set<Label>> isaExplicitCopy = new HashMap<>();
            Map<Identifier, Function<Attribute, Boolean>> predicatesCopy = new HashMap<>();
            types.forEach(((identifier, labels) -> typesCopy.put(identifier, set(labels))));
            isaExplicit.forEach(((identifier, labels) -> isaExplicitCopy.put(identifier, set(labels))));
            predicates.forEach((predicatesCopy::put));
            return new Requirements(typesCopy, isaExplicitCopy, predicatesCopy);
        }
    }
}
