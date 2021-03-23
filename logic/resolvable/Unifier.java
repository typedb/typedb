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

import grakn.common.collection.Pair;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Label;
import grakn.core.concept.Concept;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.type.ThingType;
import grakn.core.traversal.common.Identifier.Variable;
import grakn.core.traversal.common.Identifier.Variable.Retrievable;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Reasoner.REVERSE_UNIFICATION_MISSING_CONCEPT;

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
    public Optional<ConceptMap> unUnify(Map<Variable, Concept> concepts, Requirements.Instance instanceRequirements) {

        if (!unifiedRequirements.exactlySatisfiedBy(concepts)) return Optional.empty();

        Map<Retrievable, Concept> reversedConcepts = new HashMap<>();
        for (Map.Entry<Variable, Set<Retrievable>> entry : reverseUnifier.entrySet()) {
            Variable toReverse = entry.getKey();
            Set<Retrievable> reversed = entry.getValue();
            if (!concepts.containsKey(toReverse)) {
                throw GraknException.of(REVERSE_UNIFICATION_MISSING_CONCEPT, toReverse, concepts);
            }
            Concept concept = concepts.get(toReverse);
            for (Retrievable r : reversed) {
                if (!reversedConcepts.containsKey(r)) reversedConcepts.put(r, concept);
                if (!reversedConcepts.get(r).equals(concept)) return Optional.empty();
            }
        }

        if (instanceRequirements.satisfiedBy(reversedConcepts)) return Optional.of(new ConceptMap(reversedConcepts));
        else return Optional.empty();
    }

    public Map<Retrievable, Set<Variable>> mapping() {
        return unifier;
    }

    public Requirements.Constraint requirements() {
        return requirements;
    }

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

        public void add(Retrievable source, Variable target) {
            unifier.computeIfAbsent(source, (s) -> new HashSet<>()).add(target);
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
            unifier.forEach(((identifier, unifieds) -> unifierCopy.put(identifier, set(unifieds))));
            Requirements.Constraint requirementsCopy = requirements.duplicate();
            Requirements.Constraint unifiedRequirementsCopy = unifiedRequirements.duplicate();
            return new Builder(unifierCopy, requirementsCopy, unifiedRequirementsCopy);
        }
    }

    public static abstract class Requirements {

        /*
        Record constraint-based requirements that may be used to fail a unification

        Allowed requirements we may impose:
        1. a type variable may be required to within a set of allowed types
        2. a thing variable may be required to be an explicit instance of a set of allowed types
        3. a thing variable may have to satisfy a specific predicate (ie when it's an attribute)

        Note that in the future we may treat these (and variable equality constraints encoded in unifier)
         as constraints we can use to rewrite queries that are unified with. This would be more efficient,
         but query rewriting was designed _out_ of our architecture, and will have to be carefully re-added.
         */
        public static class Constraint {

            private final Map<Variable, Set<Label>> roleTypes;
            private final Map<Retrievable, Set<Label>> isaExplicit;
            private final Map<Retrievable, Function<Attribute, Boolean>> predicates;

            public Constraint() {
                this(new HashMap<>(), new HashMap<>(), new HashMap<>());
            }

            public Constraint(Map<Variable, Set<Label>> roleTypes, Map<Retrievable, Set<Label>> isaExplicit,
                              Map<Retrievable, Function<Attribute, Boolean>> predicates) {
                this.roleTypes = roleTypes;
                this.isaExplicit = isaExplicit;
                this.predicates = predicates;
            }

            public boolean exactlySatisfiedBy(Map<Variable, Concept> concepts) {
                if (!(concepts.keySet().containsAll(roleTypes.keySet()) && concepts.keySet().containsAll(isaExplicit.keySet())
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
                boolean satisfies = true;
                for (Map.Entry<Retrievable, ? extends Concept> identifiedConcept : conceptMap.concepts().entrySet()) {
                    Retrievable id = identifiedConcept.getKey();
                    Concept concept = identifiedConcept.getValue();
                    if (!(typesSatisfied(id, concept) && isaExplicitSatisfied(id, concept) && predicatesSatisfied(id, concept))) {
                        satisfies = false;
                        break;
                    }
                }
                return !satisfies;
            }

            private boolean typesSatisfied(Variable id, Concept concept) {
                if (roleTypes.containsKey(id)) {
                    assert concept.isType();
                    return roleTypes.get(id).contains(concept.asType().getLabel());
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

            public void roleTypes(Variable unifiedId, Set<Label> labels) {
                assert !roleTypes.containsKey(unifiedId) || roleTypes.get(unifiedId).equals(labels);
                roleTypes.put(unifiedId, set(labels));
            }

            public void isaExplicit(Retrievable unifiedId, Set<Label> labels) {
                assert (!isaExplicit.containsKey(unifiedId) || isaExplicit.get(unifiedId).equals(labels));
                isaExplicit.put(unifiedId, set(labels));
            }

            public void predicates(Retrievable unifiedId, Function<Attribute, Boolean> predicateFn) {
                assert !predicates.containsKey(unifiedId);
                predicates.put(unifiedId, predicateFn);
            }

            public Map<Variable, Set<Label>> roleTypes() { return roleTypes; }

            public Map<Retrievable, Set<Label>> isaExplicit() { return isaExplicit; }

            public Map<Retrievable, Function<Attribute, Boolean>> predicates() { return predicates; }

            private Constraint duplicate() {
                Map<Variable, Set<Label>> typesCopy = new HashMap<>();
                Map<Retrievable, Set<Label>> isaExplicitCopy = new HashMap<>();
                Map<Retrievable, Function<Attribute, Boolean>> predicatesCopy = new HashMap<>();
                roleTypes.forEach(((identifier, labels) -> typesCopy.put(identifier, set(labels))));
                isaExplicit.forEach(((identifier, labels) -> isaExplicitCopy.put(identifier, set(labels))));
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
