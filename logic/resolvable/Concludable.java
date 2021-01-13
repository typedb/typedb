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
import grakn.core.common.iterator.Iterators;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Label;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.Type;
import grakn.core.graph.util.Encoding;
import grakn.core.logic.LogicManager;
import grakn.core.logic.Rule;
import grakn.core.logic.tool.ConstraintCopier;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.constraint.Constraint;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint.RolePlayer;
import grakn.core.pattern.constraint.thing.ThingConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.equivalence.AlphaEquivalence;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.Variable;
import grakn.core.traversal.common.Predicate;
import graql.lang.common.GraqlToken;

import javax.annotation.Nullable;
import javax.management.relation.Role;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.map;
import static grakn.common.collection.Collections.set;
import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public abstract class Concludable<CONSTRAINT extends Constraint> extends Resolvable {

    private final CONSTRAINT constraint;
    private final Set<Constraint> coreConstraints;
    private Map<Rule, Set<Unifier>> applicableRules = null;

    private Concludable(CONSTRAINT constraint, Set<Constraint> otherCoreConstraints) {
        this.constraint = constraint;
        HashSet<Constraint> c = new HashSet<>(otherCoreConstraints);
        c.add(this.constraint);
        this.coreConstraints = set(c);
        applicableRules = new HashMap<>(); // TODO Implement
    }

    public CONSTRAINT constraint() {
        return constraint;
    }

    public Set<Constraint> coreConstraints() {
        return coreConstraints;
    }

    public static Set<Concludable<?>> create(grakn.core.pattern.Conjunction conjunction) {
        return new Extractor(conjunction.variables()).concludables();
    }

    public ResourceIterator<Unifier> getUnifiers(Rule rule) {
        assert applicableRules != null;
        return Iterators.iterate(applicableRules.get(rule));
    }

    public ResourceIterator<Rule> getApplicableRules(ConceptManager conceptMgr, LogicManager logicMgr) {
        if (applicableRules == null) computeApplicableRules(conceptMgr, logicMgr);
        return Iterators.iterate(applicableRules.keySet());
    }

    /*
    TODO this should be improved by indexing rules by possible types, so rather than retrieving all rules
    TODO and attempting to unify them, we only read rules that are relevant
     */
    private void computeApplicableRules(ConceptManager conceptMgr, LogicManager logicMgr) {
        assert applicableRules == null;
        applicableRules = new HashMap<>();
        logicMgr.rules().forEachRemaining(rule -> Iterators.iterate(unify(rule.conclusion(), conceptMgr))
                .forEachRemaining(unifier -> {
                    applicableRules.putIfAbsent(rule, new HashSet<>());
                    applicableRules.get(rule).add(unifier);
                }));
    }

    private ResourceIterator<Unifier> unify(Rule.Conclusion conclusion, ConceptManager conceptMgr) {
        if (conclusion.isRelation()) return unify(conclusion.asRelation(), conceptMgr);
        else if (conclusion.isHas()) return unify(conclusion.asHas(), conceptMgr);
        else if (conclusion.isIsa()) return unify(conclusion.asIsa(), conceptMgr);
        else if (conclusion.isValue()) return unify(conclusion.asValue(), conceptMgr);
        else throw GraknException.of(ILLEGAL_STATE);
    }

    ResourceIterator<Unifier> unify(Rule.Conclusion.Relation relationConclusion, ConceptManager conceptMgr) { return Iterators.empty(); }

    ResourceIterator<Unifier> unify(Rule.Conclusion.Has hasConclusion, ConceptManager conceptMgr) { return Iterators.empty(); }

    ResourceIterator<Unifier> unify(Rule.Conclusion.Isa isaConclusion, ConceptManager conceptMgr) { return Iterators.empty(); }

    ResourceIterator<Unifier> unify(Rule.Conclusion.Value valueConclusion, ConceptManager conceptMgr) { return Iterators.empty(); }

    public AlphaEquivalence alphaEquals(Concludable<?> that) {
        if (that.isRelation()) return alphaEquals(that.asRelation());
        else if (that.isHas()) return alphaEquals(that.asHas());
        else if (that.isIsa()) return alphaEquals(that.asIsa());
        else if (that.isAttribute()) return alphaEquals(that.asAttribute());
        else throw GraknException.of(ILLEGAL_STATE);
    }

    AlphaEquivalence alphaEquals(Relation that) { return null; }

    AlphaEquivalence alphaEquals(Has that) { return null; }

    AlphaEquivalence alphaEquals(Isa that) { return null; }

    AlphaEquivalence alphaEquals(Attribute that) { return null; }

    public boolean isRelation() { return false; }

    public boolean isHas() { return false; }

    public boolean isIsa() { return false; }

    public boolean isAttribute() { return false; }

    public Relation asRelation() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Relation.class));
    }

    public Has asHas() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Has.class));
    }

    public Isa asIsa() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Isa.class));
    }

    public Attribute asAttribute() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Attribute.class));
    }

    <T, V> Map<T, Set<V>> cloneMapping(Map<T, Set<V>> mapping) {
        Map<T, Set<V>> clone = new HashMap<>();
        mapping.forEach((key, set) -> clone.put(key, new HashSet<>(set)));
        return clone;
    }

    @Override
    public Conjunction conjunction() {
        return null; //TODO Make abstract and implement for all subtypes
    }

    /*
    Unifying a source type variable with a target type variable is impossible if none of the source's allowed labels,
    and their subtypes' labels, are found in the allowed labels of the target type.

    Improvements:
    * we could take information such as negated constraints into account.
     */
    boolean unificationSatisfiable(TypeVariable concludableTypeVar, TypeVariable conclusionTypeVar, ConceptManager conceptMgr) {

        if (!concludableTypeVar.resolvedTypes().isEmpty() && !conclusionTypeVar.resolvedTypes().isEmpty()) {
            return !Collections.disjoint(subtypeLabels(concludableTypeVar.resolvedTypes(), conceptMgr).collect(Collectors.toSet()),
                                         conclusionTypeVar.resolvedTypes());
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
    boolean unificationSatisfiable(ThingVariable concludableThingVar, ThingVariable conclusionThingVar) {
        boolean satisfiable = true;
        if (!concludableThingVar.resolvedTypes().isEmpty() && !conclusionThingVar.resolvedTypes().isEmpty()) {
            satisfiable = Collections.disjoint(concludableThingVar.resolvedTypes(), conclusionThingVar.resolvedTypes());
        }

        if (!concludableThingVar.value().isEmpty() && !conclusionThingVar.value().isEmpty()) {
            // TODO detect value contradictions between constant predicates
            satisfiable &= true;
        }
        return satisfiable;
    }

    Stream<Label> subtypeLabels(Set<Label> labels, ConceptManager conceptMgr) {
        return labels.stream().flatMap(l -> subtypeLabels(l, conceptMgr));
    }

    Stream<Label> subtypeLabels(Label label, ConceptManager conceptMgr) {
        // TODO: this is cachable, and is a hot code path - analyse and see impact of cache
        if (label.scope().isPresent()) {
            assert conceptMgr.getRelationType(label.scope().get()) != null;
            return conceptMgr.getRelationType(label.scope().get()).getRelates(label.name()).getSubtypes()
                    .map(RoleType::getLabel);
        } else {
            return conceptMgr.getThingType(label.name()).getSubtypes().map(Type::getLabel);
        }
    }

    public static class Relation extends Concludable<RelationConstraint> {

        public Relation(final RelationConstraint constraint, @Nullable IsaConstraint isaConstraint) {
            super(ConstraintCopier.copyConstraint(constraint), isaConstraint == null ? set() : set(isaConstraint));
        }

        @Override
        public ResourceIterator<Unifier> unify(Rule.Conclusion.Relation relationConclusion, ConceptManager conceptMgr) {
            if (this.constraint().players().size() > relationConclusion.relation().players().size())
                return Iterators.empty();
            Unifier.Builder unifierBuilder = Unifier.builder();

            if (!constraint().owner().reference().isAnonymous()) {
                assert constraint().owner().reference().isName();
                if (unificationSatisfiable(constraint().owner(), relationConclusion.relation().owner())) {
                    unifierBuilder.add(constraint().owner().id(), relationConclusion.relation().owner().id());
                } else return Iterators.empty();
            }

            if (constraint().owner().isa().isPresent()) {
                TypeVariable concludableRelationType = constraint().owner().isa().get().type();
                TypeVariable conclusionRelationType = relationConclusion.relation().owner().isa().get().type();
                if (unificationSatisfiable(concludableRelationType, conclusionRelationType, conceptMgr)) {
                    unifierBuilder.add(concludableRelationType.id(), conclusionRelationType.id());

                    if (concludableRelationType.reference().isLabel()) {
                        // require the unification target type variable satisfies a set of labels
                        Set<Label> allowedTypes = concludableRelationType.resolvedTypes().stream()
                                .flatMap(label -> subtypeLabels(label, conceptMgr)).collect(Collectors.toSet());
                        unifierBuilder.requirements().types(concludableRelationType.id(), allowedTypes);
                    }
                } else return Iterators.empty();
            }

            // TODO this will work for now, but we should rewrite using role player `repetition`

            List<RolePlayer> conjRolePlayers = list(constraint().players());
            List<RolePlayer> thenRolePlayers = list(relationConclusion.relation().players());

//            return matchRolePlayerIndices(conjRolePlayers, thenRolePlayers, new HashMap<>(), conceptMgr)
//                    .map(indexMap -> rolePlayerMappingToUnifier(indexMap, thenRolePlayers, unifierBuilder.duplicate(), conceptMgr));
            return matchRolePlayers(conjRolePlayers, thenRolePlayers, new HashMap<>(), conceptMgr)
                    .map(mapping -> convertMapping(mapping, unifierBuilder.duplicate(), conceptMgr));
        }

        private ResourceIterator<Map<RolePlayer, Set<RolePlayer>>> matchRolePlayers(
                List<RolePlayer> conjRolePLayers, List<RolePlayer> thenRolePlayers,
                Map<RolePlayer, Set<RolePlayer>> mapping, ConceptManager conceptMgr) {
            if (conjRolePLayers.isEmpty()) return Iterators.iterate(list(mapping));
            RolePlayer conjRP = conjRolePLayers.get(0);
            return Iterators.iterate(thenRolePlayers)
//                    .filter(thenRP -> !visited.contains(thenRP))
                    .filter(thenRP -> mapping.values().stream().noneMatch(rolePlayers -> rolePlayers.contains(thenRP)))
                    .filter(thenRP -> unificationSatisfiable(conjRP, thenRP, conceptMgr))
                    .map(thenRP -> {
//                        unifierBuilder.add(conjRP.player().id(), thenRP.player().id());
//                        if (conjRP.roleType().isPresent()) {
//                            TypeVariable roleTypeVar = conjRP.roleType().get();
//                            unifierBuilder.add(roleTypeVar.id(), thenRP.roleType().get().id());
//                            if (roleTypeVar.reference().isLabel()) {
//                                Set<Label> allowedTypes = roleTypeVar.resolvedTypes().stream()
//                                        .flatMap(roleLabel -> subtypeLabels(roleLabel, conceptMgr))
//                                        .collect(Collectors.toSet());
//                                unifierBuilder.requirements().types(roleTypeVar.id(), allowedTypes);
//                            }
//                        }
//                        Set<RolePlayer> newVisited = new HashSet<>(visited);
//                        newVisited.add(thenRP);
//                        return newVisited;
                        Map<RolePlayer, Set<RolePlayer>> clone = cloneMapping(mapping);
                        clone.putIfAbsent(conjRP, new HashSet<>());
                        clone.get(conjRP).add(thenRP);
                        return clone;
                    }).flatMap(newMapping -> matchRolePlayers(conjRolePLayers.subList(1, conjRolePLayers.size()),
                                                              thenRolePlayers, newMapping, conceptMgr));
        }

        private Unifier convertMapping(Map<RolePlayer, Set<RolePlayer>> mapping, Unifier.Builder unifierBuilder, ConceptManager conceptMgr) {
            mapping.forEach((conjRP, thenRPs) -> {
                thenRPs.forEach(thenRP -> {
                    unifierBuilder.add(conjRP.player().id(), thenRP.player().id());
                    if (conjRP.roleType().isPresent()) {
                        assert thenRP.roleType().isPresent();
                        TypeVariable conjRoleType = conjRP.roleType().get();
                        TypeVariable thenRoleType = thenRP.roleType().get();
                        unifierBuilder.add(conjRoleType.id(), thenRoleType.id());
                        if (conjRoleType.reference().isLabel()) {
                            Set<Label> allowedTypes = conjRoleType.resolvedTypes().stream()
                                    .flatMap(roleLabel -> subtypeLabels(roleLabel, conceptMgr))
                                    .collect(Collectors.toSet());
                            unifierBuilder.requirements().types(conjRoleType.id(), allowedTypes);
                        }
                    }
                });
            });

            return unifierBuilder.build();
        }

        private ResourceIterator<Map<RolePlayer, Set<Integer>>> matchRolePlayerIndices(
                List<RolePlayer> conjRolePlayers, List<RolePlayer> thenRolePlayers,
                Map<RolePlayer, Set<Integer>> mapping, ConceptManager conceptMgr) {

            if (conjRolePlayers.isEmpty()) return Iterators.iterate(list(mapping));
            RolePlayer conjRP = conjRolePlayers.get(0);

            return Iterators.iterate(IntStream.range(0, thenRolePlayers.size()).iterator())
                    .filter(thenIdx -> mapping.values().stream().noneMatch(players -> players.contains(thenIdx)))
                    .filter(thenIdx -> unificationSatisfiable(conjRP, thenRolePlayers.get(thenIdx), conceptMgr))
                    .map(thenIdx -> {
                        Map<RolePlayer, Set<Integer>> clone = cloneMapping(mapping);
                        clone.putIfAbsent(conjRP, new HashSet<>());
                        clone.get(conjRP).add(thenIdx);
                        return clone;
                    }).flatMap(newMapping -> matchRolePlayerIndices(conjRolePlayers.subList(1, conjRolePlayers.size()),
                                                                    thenRolePlayers, newMapping, conceptMgr));
        }

        private boolean unificationSatisfiable(RolePlayer concludableRolePlayer, RolePlayer conclusionRolePlayer, ConceptManager conceptMgr) {
            assert conclusionRolePlayer.roleType().isPresent();
            boolean satisfiable = true;
            if (concludableRolePlayer.roleType().isPresent()) {
                satisfiable = unificationSatisfiable(concludableRolePlayer.roleType().get(), conclusionRolePlayer.roleType().get(), conceptMgr);
            }
            satisfiable &= unificationSatisfiable(concludableRolePlayer.player(), conclusionRolePlayer.player());
            return satisfiable;
        }

        private Unifier rolePlayerMappingToUnifier(
                Map<RolePlayer, Set<Integer>> matchedRolePlayerIndices, List<RolePlayer> thenRolePlayers,
                Unifier.Builder unifierBuilder, ConceptManager conceptMgr) {

            matchedRolePlayerIndices.forEach((conjRP, thenRPIndices) -> thenRPIndices.stream().map(thenRolePlayers::get)
                    .forEach(thenRP -> {
                                 if (conjRP.roleType().isPresent()) {
                                     assert thenRP.roleType().isPresent();
                                     TypeVariable roleTypeVar = conjRP.roleType().get();
                                     unifierBuilder.add(roleTypeVar.id(), thenRP.roleType().get().id());

                                     if (roleTypeVar.reference().isLabel()) {
                                         Set<Label> allowedTypes = roleTypeVar.resolvedTypes().stream()
                                                 .flatMap(roleLabel -> subtypeLabels(roleLabel, conceptMgr))
                                                 .collect(Collectors.toSet());
                                         unifierBuilder.requirements().types(roleTypeVar.id(), allowedTypes);
                                     }
                                 }
                                 unifierBuilder.add(conjRP.player().id(), thenRP.player().id());
                             }
                    ));
            return unifierBuilder.build();
        }

        @Override
        public boolean isRelation() {
            return true;
        }

        @Override
        public Relation asRelation() {
            return this;
        }

        @Override
        AlphaEquivalence alphaEquals(Relation that) {
            return constraint().alphaEquals(that.constraint());
        }
    }

    public static class Has extends Concludable<HasConstraint> {

        public Has(final HasConstraint constraint, @Nullable IsaConstraint isaConstraint, Set<ValueConstraint<?>> valueConstraints) {
            super(ConstraintCopier.copyConstraint(constraint), core(isaConstraint, valueConstraints));
        }

        @Override
        public ResourceIterator<Unifier> unify(Rule.Conclusion.Has hasConclusion, ConceptManager conceptMgr) {
            Unifier.Builder unifierBuilder = Unifier.builder();
            if (unificationSatisfiable(constraint().owner(), hasConclusion.has().owner())) {
                unifierBuilder.add(constraint().owner().id(), hasConclusion.has().owner().id());
            } else return Iterators.empty();

            ThingVariable attr = constraint().attribute();
            if (unificationSatisfiable(attr, hasConclusion.has().attribute())) {
                unifierBuilder.add(attr.id(), hasConclusion.has().attribute().id());
                if (attr.reference().isAnonymous()) {
                    // form: $x has age 10 -> require ISA age and PREDICATE =10
                    assert attr.isa().isPresent() && attr.isa().get().type().label().isPresent();
                    Label attrLabel = attr.isa().get().type().label().get().properLabel();
                    unifierBuilder.requirements().isaExplicit(attr.id(),
                                                              subtypeLabels(attrLabel, conceptMgr).collect(Collectors.toSet()));

                    ValueConstraint<?> value = attr.value().iterator().next();
                    Function<grakn.core.concept.thing.Attribute, Boolean> predicateFn;
                    if (value.isLong()) {
                        predicateFn = (a) -> {
                            if (!Encoding.ValueType.of(a.getType().getValueType().getValueClass())
                                    .comparableTo(Encoding.ValueType.LONG)) return false;

                            assert a.getType().isDouble() || a.getType().isLong();
                            if (a.getType().isLong())
                                return value.asLong().value().compareTo(a.asLong().getValue()) == 0;
                            else if (a.getType().isDouble())
                                return Predicate.compareDoubles(a.asDouble().getValue(), value.asLong().value()) == 0;
                            else throw GraknException.of(ILLEGAL_STATE);
                        };
                    } else if (value.isDouble()) {
                        predicateFn = (a) -> {
                            if (!Encoding.ValueType.of(a.getType().getValueType().getValueClass())
                                    .comparableTo(Encoding.ValueType.DOUBLE)) return false;

                            assert a.getType().isDouble() || a.getType().isLong();
                            if (a.getType().isLong())
                                return Predicate.compareDoubles(a.asLong().getValue(), value.asDouble().value()) == 0;
                            else if (a.getType().isDouble())
                                return Predicate.compareDoubles(a.asDouble().getValue(), value.asDouble().value()) == 0;
                            else throw GraknException.of(ILLEGAL_STATE);
                        };
                    } else if (value.isBoolean()) {
                        predicateFn = (a) -> {
                            if (!Encoding.ValueType.of(a.getType().getValueType().getValueClass())
                                    .comparableTo(Encoding.ValueType.BOOLEAN)) return false;
                            assert a.getType().isBoolean();
                            return a.asBoolean().getValue().compareTo(value.asBoolean().value()) == 0;
                        };
                    } else if (value.isString()) {
                        predicateFn = (a) -> {
                            if (!Encoding.ValueType.of(a.getType().getValueType().getValueClass())
                                    .comparableTo(Encoding.ValueType.STRING)) return false;
                            assert a.getType().isString();
                            return a.asString().getValue().compareTo(value.asString().value()) == 0;
                        };
                    } else if (value.isDateTime()) {
                        predicateFn = (a) -> {
                            if (!Encoding.ValueType.of(a.getType().getValueType().getValueClass())
                                    .comparableTo(Encoding.ValueType.DATETIME)) return false;
                            assert a.getType().isDateTime();
                            return a.asDateTime().getValue().compareTo(value.asDateTime().value()) == 0;
                        };
                    } else throw GraknException.of(ILLEGAL_STATE);
                    unifierBuilder.requirements().predicates(attr.id(), predicateFn);
                } else if (attr.reference().isName() && attr.isa().isPresent() && attr.isa().get().type().label().isPresent()) {
                    // form: $x has age $a (may also handle $x has $a; $a isa age)   -> require ISA age
                    Label attrLabel = attr.isa().get().type().label().get().properLabel();
                    unifierBuilder.requirements().isaExplicit(attr.id(),
                                                              subtypeLabels(attrLabel, conceptMgr).collect(Collectors.toSet()));
                }
            } else return Iterators.empty();

            return Iterators.iterate(list(unifierBuilder.build()));
        }

        @Override
        public boolean isHas() {
            return true;
        }

        @Override
        public Has asHas() {
            return this;
        }

        @Override
        AlphaEquivalence alphaEquals(Has that) {
            return constraint().alphaEquals(that.constraint());
        }

        private static Set<Constraint> core(@Nullable IsaConstraint isaConstraint, Set<ValueConstraint<?>> valueConstraints) {
            Set<Constraint> c = new HashSet<>(valueConstraints);
            if (isaConstraint != null) c.add(isaConstraint);
            return c;
        }
    }

    public static class Isa extends Concludable<IsaConstraint> {

        public Isa(final IsaConstraint constraint, Set<ValueConstraint<?>> value) {
            super(ConstraintCopier.copyConstraint(constraint), new HashSet<>(value));
        }

        @Override
        ResourceIterator<Unifier> unify(Rule.Conclusion.Isa isaConclusion, ConceptManager conceptMgr) {
            Unifier.Builder unifierBuilder = Unifier.builder();
            if (unificationSatisfiable(constraint().owner(), isaConclusion.isa().owner())) {
                unifierBuilder.add(constraint().owner().id(), isaConclusion.isa().owner().id());
            } else return Iterators.empty();

            TypeVariable type = constraint().type();
            if (unificationSatisfiable(type, isaConclusion.isa().type(), conceptMgr)) {
                unifierBuilder.add(type.id(), isaConclusion.isa().type().id());

                if (type.reference().isLabel()) {
                    // form: $r isa friendship -> require type subs(friendship) for anonymous type variable
                    unifierBuilder.requirements().types(type.id(),
                                                        subtypeLabels(type.resolvedTypes(), conceptMgr).collect(Collectors.toSet()));
                }
            } else return Iterators.empty();

            return Iterators.iterate(list(unifierBuilder.build()));
        }

        @Override
        public boolean isIsa() {
            return true;
        }

        @Override
        public Isa asIsa() {
            return this;
        }

        @Override
        AlphaEquivalence alphaEquals(Isa that) {
            return constraint().alphaEquals(that.constraint());
        }
    }

    public static class Attribute extends Concludable<ValueConstraint<?>> {

        public Attribute(final ValueConstraint<?> constraint) {
            super(ConstraintCopier.copyConstraint(constraint), set());
        }

        @Override
        ResourceIterator<Unifier> unify(Rule.Conclusion.Value valueConclusion, ConceptManager conceptMgr) {
            Unifier.Builder unifierBuilder = Unifier.builder();
            if (unificationSatisfiable(constraint().owner(), valueConclusion.value().owner())) {
                unifierBuilder.add(constraint().owner().id(), valueConclusion.value().owner().id());
            } else return Iterators.empty();

            /*
            Interesting case:
            Unifying 'match $x >= $y' with a rule inferring a value: 'then { $x has age 10; }'
            Conceptually, $x will be 'age 10'. However, for a rule to return a valid answer for this that isn't found
            via a traversal, we require that $y also be 'age 10'. The correct unification is therefore to map both
            $x and $y to the same new inferred attribute.
            Trying to find results for a query 'match $x > $y' will never find any answers, as a rule can only infer
            an equality, ie. 'then { $x has $_age; $_age = 10; $_age isa age; }
             */
            if (constraint().isVariable()) {
                assert constraint().value() instanceof ThingVariable;
                ThingVariable value = (ThingVariable) constraint().value();
                if (unificationSatisfiable(constraint().predicate(), valueConclusion.value().predicate())) {
                    unifierBuilder.add(value.id(), valueConclusion.value().owner().id());
                } else return Iterators.empty();
                // } else {
                // form: $x > 10 -> require $x to satisfy predicate > 10
                // TODO after restructuring concludables, we whould revisit requirements on `Value` concludables
                // unifierBuilder.requirements().predicates(constraint().owner(), set(constraint()));
            }

            return Iterators.iterate(list(unifierBuilder.build()));
        }

        private boolean unificationSatisfiable(GraqlToken.Predicate concludablePredicate, GraqlToken.Predicate conclusionPredicate) {
            assert conclusionPredicate.equals(GraqlToken.Predicate.Equality.EQ);
            return !(concludablePredicate.equals(GraqlToken.Predicate.Equality.EQ) ||
                    concludablePredicate.equals(GraqlToken.Predicate.Equality.GTE) ||
                    concludablePredicate.equals(GraqlToken.Predicate.Equality.LTE));
        }

        @Override
        public boolean isAttribute() {
            return true;
        }

        @Override
        public Attribute asAttribute() {
            return this;
        }

        @Override
        AlphaEquivalence alphaEquals(Attribute that) {
            return constraint().alphaEquals(that.constraint());
        }
    }


    private static class Extractor {

        private final Set<Variable> isaOwnersToSkip = new HashSet<>();
        private final Set<Variable> valueOwnersToSkip = new HashSet<>();
        private final Set<Concludable<?>> concludables = new HashSet<>();

        Extractor(Set<Variable> variables) {
            Set<Constraint> constraints = variables.stream().flatMap(variable -> variable.constraints().stream())
                    .collect(Collectors.toSet());
            constraints.stream().filter(Constraint::isThing).map(Constraint::asThing).filter(ThingConstraint::isRelation)
                    .map(ThingConstraint::asRelation).forEach(this::fromConstraint);
            constraints.stream().filter(Constraint::isThing).map(Constraint::asThing).filter(ThingConstraint::isHas)
                    .map(ThingConstraint::asHas).forEach(this::fromConstraint);
            constraints.stream().filter(Constraint::isThing).map(Constraint::asThing).filter(ThingConstraint::isIsa)
                    .map(ThingConstraint::asIsa).forEach(this::fromConstraint);
            constraints.stream().filter(Constraint::isThing).map(Constraint::asThing).filter(ThingConstraint::isValue)
                    .map(ThingConstraint::asValue).forEach(this::fromConstraint);
        }

        public void fromConstraint(RelationConstraint relationConstraint) {
            concludables.add(new Relation(relationConstraint, relationConstraint.owner().isa().orElse(null)));
            isaOwnersToSkip.add(relationConstraint.owner());
        }

        private void fromConstraint(HasConstraint hasConstraint) {
            concludables.add(new Has(hasConstraint, hasConstraint.attribute().isa().orElse(null), hasConstraint.attribute().value()));
            isaOwnersToSkip.add(hasConstraint.attribute());
            if (hasConstraint.attribute().isa().isPresent()) valueOwnersToSkip.add(hasConstraint.attribute());
        }

        public void fromConstraint(IsaConstraint isaConstraint) {
            if (isaOwnersToSkip.contains(isaConstraint.owner())) return;
            concludables.add(new Isa(isaConstraint, isaConstraint.owner().value()));
            isaOwnersToSkip.add(isaConstraint.owner());
            valueOwnersToSkip.add(isaConstraint.owner());
        }

        private void fromConstraint(ValueConstraint<?> valueConstraint) {
            if (valueOwnersToSkip.contains(valueConstraint.owner())) return;
            concludables.add(new Attribute(valueConstraint));
        }

        public Set<Concludable<?>> concludables() {
            return new HashSet<>(concludables);
        }
    }
}
