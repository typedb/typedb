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
 */

package com.vaticle.typedb.core.logic.resolvable;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.concept.type.Type;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Reasoner.REVERSE_UNIFICATION_MISSING_CONCEPT;

public class Unifier {

    private final Map<Retrievable, Set<Variable>> unifier;
    private final Map<Variable, Set<Retrievable>> reverseUnifier;
    private final Requirements.Constraint requirements;
    private final Requirements.Constraint unifiedRequirements;

    private Unifier(Map<Retrievable, Set<Variable>> unifier, Requirements.Constraint requirements,
                    Requirements.Constraint unifiedRequirements) {
        this.unifier = Collections.unmodifiableMap(unifier);
        this.reverseUnifier = reverse(this.unifier);
        this.requirements = requirements;
        this.unifiedRequirements = unifiedRequirements;
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
    public Optional<Pair<ConceptMap, Requirements.Instance>> unify(ConceptMap conceptMap) {
        Map<Retrievable, Concept> unifiedMap = new HashMap<>();
        if (requirements.contradicts(conceptMap)) return Optional.empty();

        for (Map.Entry<Retrievable, Set<Variable>> entry : unifier.entrySet()) {
            Retrievable toUnify = entry.getKey();
            Concept concept = conceptMap.get(toUnify.asRetrievable());
            if (concept == null) continue;
            for (Variable unified : entry.getValue()) {
                if (unified.isRetrievable()) {
                    if (!unifiedMap.containsKey(unified.asRetrievable())) {
                        unifiedMap.put(unified.asRetrievable(), concept);
                    }
                    if (!unifiedMap.get(unified.asRetrievable()).equals(concept)) return Optional.empty();
                }
            }
        }
        return Optional.of(new Pair<>(new ConceptMap(unifiedMap), new Requirements.Instance(conceptMap.concepts())));
    }

    /**
     * Un-unify a map of concepts, with given identifiers. These must include anonymous and labelled concepts,
     * as they may be mapped to from a named variable, and may have requirements that need to be met.
     */
    public FunctionalIterator<ConceptMap> unUnify(Map<Variable, Concept> concepts, Requirements.Instance instanceRequirements) {

        if (!unifiedRequirements.exactlySatisfiedBy(concepts)) return Iterators.empty();

        Map<Retrievable, Concept> reversedConcepts = new HashMap<>();
        for (Map.Entry<Variable, Set<Retrievable>> entry : reverseUnifier.entrySet()) {
            Variable toReverse = entry.getKey();
            Set<Retrievable> reversed = entry.getValue();
            if (!concepts.containsKey(toReverse)) {
                throw TypeDBException.of(REVERSE_UNIFICATION_MISSING_CONCEPT, toReverse, concepts);
            }
            Concept concept = concepts.get(toReverse);
            for (Retrievable r : reversed) {
                if (!reversedConcepts.containsKey(r)) reversedConcepts.put(r, concept);
                if (!reversedConcepts.get(r).equals(concept)) return Iterators.empty();
            }
        }

        if (instanceRequirements.satisfiedBy(reversedConcepts)) return cartesianUnrestrictedNamedTypes(reversedConcepts, instanceRequirements);
        else return Iterators.empty();
    }

    private static FunctionalIterator<ConceptMap> cartesianUnrestrictedNamedTypes(Map<Retrievable, Concept> initialConcepts,
                                                                                  Requirements.Instance instanceRequirements) {
        Map<Retrievable, Concept> fixedConcepts = new HashMap<>();
        List<Variable.Name> namedTypeNames = new ArrayList<>();
        List<FunctionalIterator<Type>> namedTypeSupers = new ArrayList<>();
        initialConcepts.forEach((id, concept) -> {
            if (id.isName() && concept.isType() && !instanceRequirements.hasRestriction(id)) {
                namedTypeNames.add(id.asName());
                namedTypeSupers.add(concept.asType().getSupertypes().map(t -> t));
            } else fixedConcepts.put(id, concept);
        });

        if (namedTypeNames.isEmpty()) {
            return Iterators.single(new ConceptMap(fixedConcepts));
        } else {
            return Iterators.cartesian(namedTypeSupers).map(permutation -> {
                Map<Retrievable, Concept> concepts = new HashMap<>(fixedConcepts);
                for (int i = 0; i < permutation.size(); i++) {
                    concepts.put(namedTypeNames.get(i), permutation.get(i));
                }
                return new ConceptMap(concepts);
            });
        }
    }

    public Map<Retrievable, Set<Variable>> mapping() {
        return unifier;
    }
    public Map<Variable, Set<Retrievable>> reverseUnifier(){ return reverseUnifier; }

    public Requirements.Constraint requirements() {
        return requirements;
    }
    public Requirements.Constraint unifiedRequirements(){ return unifiedRequirements;}

    private Map<Variable, Set<Retrievable>> reverse(Map<Retrievable, Set<Variable>> unifier) {
        Map<Variable, Set<Retrievable>> reverse = new HashMap<>();
        unifier.forEach((unify, unifieds) -> {
            for (Variable unified : unifieds) {
                reverse.computeIfAbsent(unified, (u) -> new HashSet<>()).add(unify);
            }
        });
        return reverse;
    }

    @Override
    public String toString() {
        return "Unifier{" +
                "unifier=" + unifierString(unifier) +
                ", reverseUnifier=" + unifierString(reverseUnifier) +
                ", requirements=" + requirements +
                ", unifiedRequirements=" + unifiedRequirements +
                '}';
    }

    private String unifierString(Map<? extends Variable, ? extends Set<? extends Variable>> unifier) {
        return "{" + unifier.entrySet().stream().map(e -> e.getKey() + "->" + e.getValue()).collect(Collectors.joining(",")) + "}";
    }

    public static class Builder {

        private Map<Retrievable, Set<Variable>> unifier;
        private Requirements.Constraint requirements;
        private Requirements.Constraint unifiedRequirements;

        public Builder() {
            this(new HashMap<>(), new Requirements.Constraint(), new Requirements.Constraint());
        }

        private Builder(Map<Retrievable, Set<Variable>> unifier, Requirements.Constraint requirements, Requirements.Constraint unifiedRequirements) {
            this.unifier = unifier;
            this.requirements = requirements;
            this.unifiedRequirements = unifiedRequirements;
        }

        public void addThing(com.vaticle.typedb.core.pattern.variable.ThingVariable source, Retrievable target) {
            unifier.computeIfAbsent(source.id(), (s) -> new HashSet<>()).add(target);
            requirements.isaExplicit(source.id(), source.resolvedTypes());
            unifiedRequirements.isaExplicit(target, source.resolvedTypes());
        }

        public void addVariableType(com.vaticle.typedb.core.pattern.variable.TypeVariable source, Variable target) {
            assert source.id().isVariable();
            unifier.computeIfAbsent(source.id().asRetrievable(), (s) -> new HashSet<>()).add(target);
            requirements.types(source.id(), source.resolvedTypes());
            unifiedRequirements.types(target, source.resolvedTypes());
        }

        public void addLabelType(Identifier.Variable.Label source, Set<Label> allowedTypes, Variable target) {
            requirements.types(source, allowedTypes);
            unifiedRequirements.types(target, allowedTypes);
        }

        public Requirements.Constraint requirements() {
            return requirements;
        }

        public Requirements.Constraint unifiedRequirements() {
            return unifiedRequirements;
        }

        public Unifier build() {
            return new Unifier(unifier, requirements, unifiedRequirements);
        }

        public Builder clone() {
            Map<Retrievable, Set<Variable>> unifierCopy = new HashMap<>();
            unifier.forEach(((identifier, unifieds) -> unifierCopy.put(identifier, new HashSet<>(unifieds))));
            Requirements.Constraint requirementsCopy = requirements.duplicate();
            Requirements.Constraint unifiedRequirementsCopy = unifiedRequirements.duplicate();
            return new Builder(unifierCopy, requirementsCopy, unifiedRequirementsCopy);
        }
    }

    public static abstract class Requirements {

        /*
        Record constraint-based requirements that may be used to fail a unification

        Allowed requirements we may impose:
        1. a role type variable may be required to within a set of allowed role types
        2. a thing variable may be required to be an explicit instance of a set of allowed types
        3. a thing variable may have to satisfy a specific predicate (ie when it's an attribute)

        Note that in the future we may treat these (and variable equality constraints encoded in unifier)
         as constraints we can use to rewrite queries that are unified with. This would be more efficient,
         but query rewriting was designed _out_ of our architecture, and will have to be carefully re-added.
         */
        public static class Constraint {

            private final Map<Variable, Set<Label>> types;
            private final Map<Retrievable, Set<Label>> isaExplicit;
            private final Map<Retrievable, Function<Attribute, Boolean>> predicates;

            public Constraint() {
                this(new HashMap<>(), new HashMap<>(), new HashMap<>());
            }

            public Constraint(Map<Variable, Set<Label>> types, Map<Retrievable, Set<Label>> isaExplicit,
                              Map<Retrievable, Function<Attribute, Boolean>> predicates) {
                this.types = types;
                this.isaExplicit = isaExplicit;
                this.predicates = predicates;
            }

            public boolean exactlySatisfiedBy(Map<Variable, Concept> concepts) {
                if (!(concepts.keySet().containsAll(types.keySet()) && concepts.keySet().containsAll(isaExplicit.keySet())
                        && concepts.keySet().containsAll(predicates.keySet()))) {
                    return false;
                }

                for (Map.Entry<Variable, Concept> identifiedConcept : concepts.entrySet()) {
                    Variable id = identifiedConcept.getKey();
                    Concept concept = identifiedConcept.getValue();
                    if (!(typesSatisfied(id, concept) && isaExplicitSatisfied(id, concept) && predicatesSatisfied(id, concept))) {
                        return false;
                    }
                }
                return true;
            }

            public boolean contradicts(ConceptMap conceptMap) {
                for (Map.Entry<Retrievable, ? extends Concept> identifiedConcept : conceptMap.concepts().entrySet()) {
                    Retrievable id = identifiedConcept.getKey();
                    Concept concept = identifiedConcept.getValue();
                    if (!(typesSatisfied(id, concept) && isaExplicitSatisfied(id, concept) && predicatesSatisfied(id, concept))) {
                        return true;
                    }
                }
                return false;
            }

            private boolean typesSatisfied(Variable id, Concept concept) {
                if (types.containsKey(id)) {
                    assert concept.isType();
                    return types.get(id).contains(concept.asType().getLabel());
                } else {
                    return true;
                }
            }

            private boolean isaExplicitSatisfied(Variable id, Concept concept) {
                if (id.isRetrievable() && isaExplicit.containsKey(id.asRetrievable())) {
                    assert concept.isThing();
                    ThingType type = concept.asThing().getType();
                    return isaExplicit.get(id.asRetrievable()).contains(type.getLabel());
                } else {
                    return true;
                }
            }

            private boolean predicatesSatisfied(Variable id, Concept concept) {
                if (id.isRetrievable() && predicates.containsKey(id.asRetrievable())) {
                    assert concept.isAttribute();
                    return predicates.get(id.asRetrievable()).apply(concept.asAttribute());
                } else {
                    return true;
                }
            }

            private void types(Variable id, Set<Label> labels) {
                assert !types.containsKey(id) || types.get(id).equals(labels);
                types.put(id, set(labels));
            }

            private void isaExplicit(Retrievable id, Set<Label> labels) {
                if (isaExplicit.containsKey(id)) {
                    isaExplicit.compute(id, (i, existingLabels) -> {
                        existingLabels.retainAll(labels);
                        return existingLabels;
                    });
                } else {
                    isaExplicit.put(id, new HashSet<>(labels));
                }
            }

            public void predicates(Retrievable id, Function<Attribute, Boolean> predicateFn) {
                assert !predicates.containsKey(id);
                predicates.put(id, predicateFn);
            }

            public Map<Variable, Set<Label>> types() { return types; }

            public Map<Retrievable, Set<Label>> isaExplicit() { return isaExplicit; }

            public Map<Retrievable, Function<Attribute, Boolean>> predicates() { return predicates; }

            private Constraint duplicate() {
                Map<Variable, Set<Label>> typesCopy = new HashMap<>();
                Map<Retrievable, Set<Label>> isaExplicitCopy = new HashMap<>();
                Map<Retrievable, Function<Attribute, Boolean>> predicatesCopy = new HashMap<>();
                types.forEach(((identifier, labels) -> typesCopy.put(identifier, set(labels))));
                isaExplicit.forEach(((identifier, labels) -> isaExplicitCopy.put(identifier, new HashSet<>(labels))));
                predicates.forEach((predicatesCopy::put));
                return new Constraint(typesCopy, isaExplicitCopy, predicatesCopy);
            }
        }

        public static class Instance {

            Map<Retrievable, ? extends Concept> requireCompatible;
            private final int hash;

            public Instance(Map<Retrievable, ? extends Concept> concepts) {
                this.requireCompatible = concepts;
                this.hash = Objects.hash(requireCompatible);
            }

            public boolean satisfiedBy(Map<Retrievable, Concept> toTest) {
                for (Map.Entry<Retrievable, ? extends Concept> entry : toTest.entrySet()) {
                    Retrievable id = entry.getKey().asRetrievable();
                    Concept compatible = requireCompatible.get(id);
                    if (compatible != null) {
                        Concept testConcept = entry.getValue();
                        // things must be exactly equal
                        if ((compatible.isThing() && !compatible.equals(testConcept)) ||
                                // if the required concept is a type, the test concept must also be a type
                                (compatible.isType() && !testConcept.isType()) ||
                                // types must be compatible (testConcept must be a subtype of required concept)
                                (compatible.isType() && testConcept.asType().getSupertypes().noneMatch(t -> t.equals(compatible)))) {
                            return false;
                        }
                    }
                }
                return true;
            }

            public boolean hasRestriction(Retrievable var) {
                return requireCompatible.containsKey(var);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Instance instance = (Instance) o;
                return requireCompatible.equals(instance.requireCompatible);
            }

            @Override
            public int hashCode() {
                return hash;
            }
        }
    }
}
