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
 */

package com.vaticle.typedb.core.logic.resolvable;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.impl.AttributeImpl;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.concept.type.Type;
import com.vaticle.typedb.core.pattern.constraint.thing.RelationConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.ValueConstraint;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import com.vaticle.typedb.core.traversal.predicate.Predicate;
import com.vaticle.typedb.core.traversal.predicate.PredicateArgument;
import com.vaticle.typedb.core.traversal.predicate.PredicateOperator;

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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Reasoner.REVERSE_UNIFICATION_MISSING_CONCEPT;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typeql.lang.common.TypeQLToken.Predicate.Equality.EQ;

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

        if (instanceRequirements.satisfiedBy(reversedConcepts)) {
            return cartesianUnrestrictedNamedTypes(reversedConcepts, instanceRequirements);
        } else return Iterators.empty();
    }

    private static FunctionalIterator<ConceptMap> cartesianUnrestrictedNamedTypes(Map<Retrievable, Concept> initialConcepts,
                                                                                  Requirements.Instance instanceRequirements) {
        Map<Retrievable, Concept> fixedConcepts = new HashMap<>();
        List<Variable.Name> namedTypeNames = new ArrayList<>();
        List<FunctionalIterator<Type>> namedTypeSupers = new ArrayList<>();
        initialConcepts.forEach((id, concept) -> {
            if (id.isName() && concept.isType()) {
                namedTypeNames.add(id.asName());
                if (!instanceRequirements.hasRestriction(id)) {
                    namedTypeSupers.add(concept.asType().getSupertypes().map(t -> t));
                } else {
                    namedTypeSupers.add(concept.asType().getSupertypes()
                            .filter(t -> t.equals(instanceRequirements.restriction(id)))
                            .map(t -> t));
                }
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

    public Map<Variable, Set<Retrievable>> reverseUnifier() {
        return reverseUnifier;
    }

    public Requirements.Constraint requirements() {
        return requirements;
    }

    public Requirements.Constraint unifiedRequirements() {
        return unifiedRequirements;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Unifier unifier1 = (Unifier) o;
        return unifier.equals(unifier1.unifier) &&
                reverseUnifier.equals(unifier1.reverseUnifier) &&
                requirements.equals(unifier1.requirements) &&
                unifiedRequirements.equals(unifier1.unifiedRequirements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(unifier, reverseUnifier, requirements, unifiedRequirements);
    }

    public static class Builder {

        private final Map<Retrievable, Set<Variable>> unifier;
        private final Requirements.Constraint requirements;
        private final Requirements.Constraint unifiedRequirements;

        private Builder() {
            this(new HashMap<>(), new Requirements.Constraint(), new Requirements.Constraint());
        }

        private Builder(Map<Retrievable, Set<Variable>> unifier, Requirements.Constraint requirements, Requirements.Constraint unifiedRequirements) {
            this.unifier = unifier;
            this.requirements = requirements;
            this.unifiedRequirements = unifiedRequirements;
        }

        public void addThing(com.vaticle.typedb.core.pattern.variable.ThingVariable source, Retrievable target) {
            unifier.computeIfAbsent(source.id(), (s) -> new HashSet<>()).add(target);
            requirements.isaExplicit(source.id(), source.inferredTypes());
            unifiedRequirements.isaExplicit(target, source.inferredTypes());
        }

        public void addVariableType(com.vaticle.typedb.core.pattern.variable.TypeVariable source, Variable target) {
            assert source.id().isVariable();
            unifier.computeIfAbsent(source.id().asRetrievable(), (s) -> new HashSet<>()).add(target);
            requirements.types(source.id(), source.inferredTypes());
            unifiedRequirements.types(target, source.inferredTypes());
        }

        public void addLabelType(Identifier.Variable.Label source, Set<Label> allowedTypes, Variable target) {
            requirements.types(source, allowedTypes);
            unifiedRequirements.types(target, allowedTypes);
        }

        void addConstantValueRequirements(Set<ValueConstraint<?>> values, Retrievable id, Retrievable unifiedId) {
            for (ValueConstraint.Constant<?> value : constantValueConstraints(values)) {
                unifiedRequirements().predicates(unifiedId, valuePredicate(value));
                requirements().predicates(id, valuePredicate(value));
            }
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

        static FunctionalIterator<Label> subtypeLabels(Set<Label> labels, ConceptManager conceptMgr) {
            return iterate(labels).flatMap(l -> subtypeLabels(l, conceptMgr));
        }

        static FunctionalIterator<Label> subtypeLabels(Label label, ConceptManager conceptMgr) {
            // TODO: this is cachable, and is a hot code path - analyse and see impact of cache
            if (label.scope().isPresent()) {
                assert conceptMgr.getRelationType(label.scope().get()) != null;
                return conceptMgr.getRelationType(label.scope().get()).getRelates(label.name()).getSubtypes()
                        .map(RoleType::getLabel);
            } else {
                return conceptMgr.getThingType(label.name()).getSubtypes().map(Type::getLabel);
            }
        }

        /*
        Unifying a source type variable with a target type variable is impossible if none of the source's allowed labels,
        and their subtypes' labels, are found in the allowed labels of the target type.

        Improvements:
        * we could take information such as negated constraints into account.
         */
        static boolean unificationSatisfiable(TypeVariable concludableTypeVar, TypeVariable conclusionTypeVar, ConceptManager conceptMgr) {
            if (!concludableTypeVar.inferredTypes().isEmpty() && !conclusionTypeVar.inferredTypes().isEmpty()) {
                return !Collections.disjoint(subtypeLabels(concludableTypeVar.inferredTypes(), conceptMgr).toSet(),
                        conclusionTypeVar.inferredTypes());
            } else {
                // if either variable is allowed to be any type (ie empty set), its possible to do unification
                return true;
            }
        }

        /*
        Unifying a source thing variable with a target thing variable is impossible if none of the source's
        allowed types are found in the target's allowed types.
        It is also impossible, if there are value constraints on the source that are incompatible with value constraints
        on the target. eg. `$x > 10` and `$x = 5`.

        Improvements:
        * take into account negated constraints
        * take into account if an attribute owned is a key but the unification target requires a different value
         */
        static boolean unificationSatisfiable(ThingVariable concludableThingVar, ThingVariable conclusionThingVar) {
            boolean satisfiable = true;
            if (!concludableThingVar.inferredTypes().isEmpty() && !conclusionThingVar.inferredTypes().isEmpty()) {
                satisfiable = !Collections.disjoint(concludableThingVar.inferredTypes(), conclusionThingVar.inferredTypes());
            }

            if (!concludableThingVar.value().isEmpty() && !conclusionThingVar.value().isEmpty()) {
                assert conclusionThingVar.value().size() == 1;
                ValueConstraint<?> conclusionValueConstraint = iterate(conclusionThingVar.value()).next();

                assert conclusionValueConstraint.predicate().isEquality() &&
                        conclusionValueConstraint.predicate().asEquality().equals(EQ);

                satisfiable &= iterate(concludableThingVar.value()).allMatch(v -> !v.inconsistentWith(conclusionValueConstraint));
            }
            return satisfiable;
        }

        static boolean unificationSatisfiable(RelationConstraint.RolePlayer concludableRolePlayer, RelationConstraint.RolePlayer conclusionRolePlayer, ConceptManager conceptMgr) {
            assert conclusionRolePlayer.roleType().isPresent();
            boolean satisfiable = true;
            if (concludableRolePlayer.roleType().isPresent()) {
                satisfiable = unificationSatisfiable(concludableRolePlayer.roleType().get(), conclusionRolePlayer.roleType().get(), conceptMgr);
            }
            satisfiable &= unificationSatisfiable(concludableRolePlayer.player(), conclusionRolePlayer.player());
            return satisfiable;
        }

        static Set<? extends ValueConstraint.Constant<?>> constantValueConstraints(Set<ValueConstraint<?>> values) {
            return iterate(values).filter(ValueConstraint::isConstant).map(ValueConstraint::asConstant).toSet();
        }

        static <T> Function<com.vaticle.typedb.core.concept.thing.Attribute, Boolean> valuePredicate(ValueConstraint.Constant<T> value) {
            assert !value.isVariable();
            if (value.isString()) {
                if (value.predicate().isEquality()) {
                    return (a) -> Predicate.Value.String.of(value.predicate()).apply(((AttributeImpl<?>) a).readableVertex(), value.asString().value());
                } else {
                    PredicateOperator.SubString<?> operator = PredicateOperator.SubString.of(value.predicate().asSubString());
                    if (operator == PredicateOperator.SubString.CONTAINS) {
                        return (a) -> PredicateOperator.SubString.CONTAINS.apply(a.asString().getValue(), value.asString().value());
                    } else if (operator == PredicateOperator.SubString.LIKE) {
                        return (a) -> PredicateOperator.SubString.LIKE.apply(a.asString().getValue(), Pattern.compile(value.asString().value()));
                    } else throw TypeDBException.of(ILLEGAL_STATE);
                }
            } else if (value.isLong()) {
                return (a) -> Predicate.Value.Numerical.of(value.predicate().asEquality(), PredicateArgument.Value.LONG)
                        .apply(((AttributeImpl<?>) a).readableVertex(), value.asLong().value());
            } else if (value.isDouble()) {
                return (a) -> Predicate.Value.Numerical.of(value.predicate().asEquality(), PredicateArgument.Value.DOUBLE)
                        .apply(((AttributeImpl<?>) a).readableVertex(), value.asDouble().value());
            } else if (value.isBoolean()) {
                return (a) -> Predicate.Value.Numerical.of(value.predicate().asEquality(), PredicateArgument.Value.BOOLEAN)
                        .apply(((AttributeImpl<?>) a).readableVertex(), value.asBoolean().value());
            } else if (value.isDateTime()) {
                return (a) -> Predicate.Value.Numerical.of(value.predicate().asEquality(), PredicateArgument.Value.DATETIME)
                        .apply(((AttributeImpl<?>) a).readableVertex(), value.asDateTime().value());
            } else throw TypeDBException.of(ILLEGAL_STATE);
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
                    Set<Label> satisfyingTypes = new HashSet<>(types.get(id));
                    satisfyingTypes.retainAll(iterate(concept.asType().getSupertypes()).map(Type::getLabel).toSet());
                    return satisfyingTypes.size() > 0;
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

            public Map<Variable, Set<Label>> types() {
                return types;
            }

            public Map<Retrievable, Set<Label>> isaExplicit() {
                return isaExplicit;
            }

            public Map<Retrievable, Function<Attribute, Boolean>> predicates() {
                return predicates;
            }

            private Constraint duplicate() {
                Map<Variable, Set<Label>> typesCopy = new HashMap<>();
                Map<Retrievable, Set<Label>> isaExplicitCopy = new HashMap<>();
                Map<Retrievable, Function<Attribute, Boolean>> predicatesCopy = new HashMap<>();
                types.forEach(((identifier, labels) -> typesCopy.put(identifier, set(labels))));
                isaExplicit.forEach(((identifier, labels) -> isaExplicitCopy.put(identifier, new HashSet<>(labels))));
                predicates.forEach((predicatesCopy::put));
                return new Constraint(typesCopy, isaExplicitCopy, predicatesCopy);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Constraint that = (Constraint) o;
                return types.equals(that.types) &&
                        isaExplicit.equals(that.isaExplicit) &&
                        predicates.equals(that.predicates);
            }

            @Override
            public int hashCode() {
                return Objects.hash(types, isaExplicit, predicates);
            }
        }

        public static class Instance {

            private final Map<Retrievable, ? extends Concept> requireCompatible;
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

            public Concept restriction(Retrievable var) {
                assert hasRestriction(var);
                return requireCompatible.get(var);
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
