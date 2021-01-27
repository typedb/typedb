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
import grakn.core.graph.common.Encoding;
import grakn.core.logic.LogicManager;
import grakn.core.logic.Rule;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.constraint.Constraint;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint.RolePlayer;
import grakn.core.pattern.constraint.thing.ThingConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.constraint.type.LabelConstraint;
import grakn.core.pattern.equivalence.AlphaEquivalence;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.Variable;
import grakn.core.traversal.common.Identifier;
import grakn.core.traversal.predicate.Predicate;
import graql.lang.common.GraqlToken;
import graql.lang.pattern.variable.Reference;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.set;
import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.common.iterator.Iterators.single;
import static graql.lang.common.GraqlToken.Predicate.Equality.EQ;

public abstract class Concludable extends Resolvable {

    private Map<Rule, Set<Unifier>> applicableRules;

    private Concludable(Conjunction conjunction) {
        super(conjunction);
        applicableRules = null;
    }

    public boolean isConcludable() {
        return true;
    }

    public Concludable asConcludable() {
        return this;
    }

    public abstract Set<Constraint> concludableConstraints();

    public static Set<Concludable> create(grakn.core.pattern.Conjunction conjunction) {
        return new Extractor(conjunction).concludables();
    }

    public ResourceIterator<Unifier> getUnifiers(Rule rule) {
        assert applicableRules != null;
        return iterate(applicableRules.get(rule));
    }

    public ResourceIterator<Rule> getApplicableRules(ConceptManager conceptMgr, LogicManager logicMgr) {
        synchronized (this) {
            if (applicableRules == null) applicableRules = applicableRules(conceptMgr, logicMgr);
        }
        return Iterators.iterate(applicableRules.keySet());
    }

    abstract Map<Rule, Set<Unifier>> applicableRules(ConceptManager conceptMgr, LogicManager logicMgr);

    public abstract Variable generating();

    abstract ResourceIterator<Unifier> unify(Rule.Conclusion conclusion, ConceptManager conceptMgr);

    public abstract AlphaEquivalence alphaEquals(Concludable that);

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
            satisfiable = !Collections.disjoint(concludableThingVar.resolvedTypes(), conclusionThingVar.resolvedTypes());
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

    private static Set<ValueConstraint<?>> equalsConstraints(Set<ValueConstraint<?>> values) {
        return iterate(values).filter(v -> v.predicate().equals(EQ)).toSet();
    }

    private static Function<grakn.core.concept.thing.Attribute, Boolean> valueEqualsFunction(ValueConstraint<?> value) {
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
        return predicateFn;
    }

    protected void addValueEqualsRequirements(Unifier.Builder unifierBuilder, Set<ValueConstraint<?>> values) {
        for (ValueConstraint<?> value : equalsConstraints(values)) {
            unifierBuilder.requirements().predicates(value.owner().id(), valueEqualsFunction(value));
        }
    }

    /**
     * Relation handles these concludable patterns, where `$role` and `$relation` could be labelled, and there could
     * be any number of rolePlayers:
     * { $r($role: $x) isa $relation; }
     * { $r($role: $x); }
     * { $r($x) isa $relation; }
     * { $r($x); }
     * { ($role: $x) isa $relation; }
     * { ($x) isa $relation; }
     * { ($x); }
     */
    public static class Relation extends Concludable {

        private final RelationConstraint relation;
        private final IsaConstraint isa;
        private final Set<LabelConstraint> labels;

        private Relation(Conjunction conjunction, RelationConstraint relation, @Nullable IsaConstraint isa, Set<LabelConstraint> labels) {
            super(conjunction);
            this.relation = relation;
            this.isa = isa;
            this.labels = labels;
        }

        public static Relation of(RelationConstraint relation, @Nullable IsaConstraint isa, Set<LabelConstraint> labels) {
            Conjunction.Cloner cloner;
            IsaConstraint clonedIsa;
            if (isa == null) {
                cloner = Conjunction.Cloner.cloneExactly(labels, relation);
                clonedIsa = null;
            } else {
                cloner = Conjunction.Cloner.cloneExactly(labels, isa, relation);
                clonedIsa = cloner.getClone(isa).asThing().asIsa();
            }
            return new Relation(cloner.conjunction(), cloner.getClone(relation).asThing().asRelation(), clonedIsa,
                                iterate(labels).map(l -> cloner.getClone(l).asType().asLabel()).toSet());
        }

        public RelationConstraint relation() {
            return relation;
        }

        @Override
        public Set<Constraint> concludableConstraints() {
            Set<Constraint> c = new HashSet<>(labels);
            c.add(relation);
            if (isa != null) c.add(isa);
            return set(c);
        }

        @Override
        ResourceIterator<Unifier> unify(Rule.Conclusion conclusion, ConceptManager conceptMgr) {
            if (conclusion.isRelation()) return unify(conclusion.asRelation(), conceptMgr);
            return Iterators.empty();
        }

        public ResourceIterator<Unifier> unify(Rule.Conclusion.Relation relationConclusion, ConceptManager conceptMgr) {
            if (this.relation().players().size() > relationConclusion.relation().players().size())
                return Iterators.empty();
            Unifier.Builder unifierBuilder = Unifier.builder();

            if (!relation().owner().reference().isAnonymous()) {
                assert relation().owner().reference().isName();
                if (unificationSatisfiable(relation().owner(), relationConclusion.relation().owner())) {
                    unifierBuilder.add(relation().owner().id(), relationConclusion.relation().owner().id());
                } else return Iterators.empty();
            }

            if (relation().owner().isa().isPresent()) {
                TypeVariable concludableRelationType = relation().owner().isa().get().type();
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

            List<RolePlayer> conjRolePlayers = list(relation().players());
            Set<RolePlayer> thenRolePlayers = relationConclusion.relation().players();

            return matchRolePlayers(conjRolePlayers, thenRolePlayers, new HashMap<>(), conceptMgr)
                    .map(mapping -> convertRPMappingToUnifier(mapping, unifierBuilder.duplicate(), conceptMgr));
        }

        @Override
        public Variable generating() {
            return relation().owner();
        }

        private ResourceIterator<Map<RolePlayer, Set<RolePlayer>>> matchRolePlayers(
                List<RolePlayer> conjRolePLayers, Set<RolePlayer> thenRolePlayers,
                Map<RolePlayer, Set<RolePlayer>> mapping, ConceptManager conceptMgr) {
            if (conjRolePLayers.isEmpty()) return single(mapping);
            RolePlayer conjRP = conjRolePLayers.get(0);
            return iterate(thenRolePlayers)
                    .filter(thenRP -> iterate(mapping.values()).noneMatch(rolePlayers -> rolePlayers.contains(thenRP)))
                    .filter(thenRP -> unificationSatisfiable(conjRP, thenRP, conceptMgr))
                    .map(thenRP -> {
                        Map<RolePlayer, Set<RolePlayer>> clone = cloneMapping(mapping);
                        clone.putIfAbsent(conjRP, new HashSet<>());
                        clone.get(conjRP).add(thenRP);
                        return clone;
                    }).flatMap(newMapping -> matchRolePlayers(conjRolePLayers.subList(1, conjRolePLayers.size()),
                                                              thenRolePlayers, newMapping, conceptMgr));
        }

        private Unifier convertRPMappingToUnifier(
                Map<RolePlayer, Set<RolePlayer>> mapping, Unifier.Builder unifierBuilder, ConceptManager conceptMgr) {
            mapping.forEach((conjRP, thenRPs) -> thenRPs.forEach(thenRP -> {
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
            }));

            return unifierBuilder.build();
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

        @Override
        Map<Rule, Set<Unifier>> applicableRules(ConceptManager conceptMgr, LogicManager logicMgr) {
            Variable generatedRelation = generating();
            Set<Label> relationTypes = generatedRelation.resolvedTypes();
            // may never be empty as its always known to be at least a relation
            assert generatedRelation.isSatisfiable();

            Map<Rule, Set<Unifier>> applicableRules = new HashMap<>();
            relationTypes.forEach(type -> logicMgr.rulesConcluding(type)
                    .forEachRemaining(rule -> unify(rule.conclusion(), conceptMgr)
                            .forEachRemaining(unifier -> {
                                applicableRules.putIfAbsent(rule, new HashSet<>());
                                applicableRules.get(rule).add(unifier);
                            })));

            return applicableRules;
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
        public AlphaEquivalence alphaEquals(Concludable that) {
            if (!that.isRelation()) return AlphaEquivalence.invalid();
            return relation().alphaEquals(that.asRelation().relation());
        }
    }

    /**
     * `Has` handles these concludable patterns:
     * `{ $x has $a; }`,
     * `{ $x has age $a; }`,
     * `{ $x has age 30; }`,
     * `{ $x has $a; $a isa age; }`,
     * `{ $x has $a; $a 30 isa age; }`,
     * `{ $x has $a; $a = 30; }`
     * `{ $x has $a; $a < 30; $a >= 10; }`
     * Value constraints are included here to improve performance, with the exception of `=`, which cannot be included
     * elsewhere due to the anonymous attribute variable in examples such as `{ $x has age 30; }`. Only `=` is added as
     * a requirement on the unifier.
     */
    public static class Has extends Concludable {

        private final HasConstraint has;
        private final IsaConstraint isa;
        private final Set<ValueConstraint<?>> values;

        private Has(Conjunction conjunction, HasConstraint has, @Nullable IsaConstraint isa, Set<ValueConstraint<?>> values) {
            super(conjunction);
            this.has = has;
            this.isa = isa;
            this.values = values;
        }

        public static Has of(HasConstraint has, @Nullable IsaConstraint isa, Set<ValueConstraint<?>> values, Set<LabelConstraint> labels) {
            Conjunction.Cloner cloner;
            IsaConstraint clonedIsa;
            if (isa == null) {
                cloner = Conjunction.Cloner.cloneExactly(values, has);
                clonedIsa = null;
            } else {
                cloner = Conjunction.Cloner.cloneExactly(labels, values, isa, has);
                clonedIsa = cloner.getClone(isa).asThing().asIsa();
            }
            ResourceIterator<ValueConstraint<?>> valueIt = iterate(values).map(cloner::getClone).map(c -> c.asThing().asValue());
            return new Has(cloner.conjunction(), cloner.getClone(has).asThing().asHas(), clonedIsa, valueIt.toSet());
        }

        public HasConstraint has() {
            return has;
        }

        @Override
        public Set<Constraint> concludableConstraints() {
            Set<Constraint> constraints = new HashSet<>();
            constraints.add(has);
            if (isa != null) constraints.add(isa);
            constraints.addAll(equalsConstraints(values));
            return set(constraints);
        }

        @Override
        ResourceIterator<Unifier> unify(Rule.Conclusion conclusion, ConceptManager conceptMgr) {
            if (conclusion.isHas()) return unify(conclusion.asHas(), conceptMgr);
            return Iterators.empty();
        }

        public ResourceIterator<Unifier> unify(Rule.Conclusion.Has hasConclusion, ConceptManager conceptMgr) {
            Unifier.Builder unifierBuilder = Unifier.builder();
            if (unificationSatisfiable(has().owner(), hasConclusion.has().owner())) {
                unifierBuilder.add(has().owner().id(), hasConclusion.has().owner().id());
            } else return Iterators.empty();

            ThingVariable attr = has().attribute();
            if (unificationSatisfiable(attr, hasConclusion.has().attribute())) {
                unifierBuilder.add(attr.id(), hasConclusion.has().attribute().id());
                if (attr.reference().isAnonymous()) {
                    // form: $x has age 10 -> require ISA age and PREDICATE =10
                    assert attr.isa().isPresent() && attr.isa().get().type().label().isPresent();
                    Label attrLabel = attr.isa().get().type().label().get().properLabel();
                    unifierBuilder.requirements().isaExplicit(attr.id(),
                                                              subtypeLabels(attrLabel, conceptMgr).collect(Collectors.toSet()));
                    addValueEqualsRequirements(unifierBuilder, values);
                } else if (attr.reference().isName() && attr.isa().isPresent() && attr.isa().get().type().label().isPresent()) {
                    // form: $x has age $a (may also handle $x has $a; $a isa age)   -> require ISA age
                    Label attrLabel = attr.isa().get().type().label().get().properLabel();
                    unifierBuilder.requirements().isaExplicit(attr.id(),
                                                              subtypeLabels(attrLabel, conceptMgr).collect(Collectors.toSet()));
                }
            } else return Iterators.empty();

            return single(unifierBuilder.build());
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
        public Variable generating() {
            return has.attribute();
        }

        @Override
        Map<Rule, Set<Unifier>> applicableRules(ConceptManager conceptMgr, LogicManager logicMgr) {
            Variable attribute = generating();
            Set<Label> attributeTypes = attribute.resolvedTypes();
            // may never be empty as its always known to be at least an attribute
            assert attribute.isSatisfiable();

            Map<Rule, Set<Unifier>> applicableRules = new HashMap<>();
            attributeTypes.forEach(type -> logicMgr.rulesConcludingHas(type)
                    .forEachRemaining(rule -> unify(rule.conclusion(), conceptMgr)
                            .forEachRemaining(unifier -> {
                                applicableRules.putIfAbsent(rule, new HashSet<>());
                                applicableRules.get(rule).add(unifier);
                            })));

            return applicableRules;
        }

        @Override
        public AlphaEquivalence alphaEquals(Concludable that) {
            if (!that.isHas()) return AlphaEquivalence.invalid();
            return has().alphaEquals(that.asHas().has());
        }

    }

    /**
     * Isa handles concludable these concludable patterns, where the owner of the IsaConstraint is not already the
     * subject of a Has or Relation Concludable:
     * `{ $x isa person; }`
     * `{ $a isa age; $a = 30; }`
     * `{ $a isa age; $a > 5; $a < 30; }`
     * Value constraints are included here to improve performance. Only `=` is added as a requirement on the unifier.
     */
    public static class Isa extends Concludable {

        private final IsaConstraint isa;
        private final Set<ValueConstraint<?>> values;

        private Isa(Conjunction conjunction, IsaConstraint isa, Set<ValueConstraint<?>> values) {
            super(conjunction);
            this.isa = isa;
            this.values = values;
        }

        public static Isa of(IsaConstraint isa, Set<ValueConstraint<?>> values, Set<LabelConstraint> labelConstraints) {
            Conjunction.Cloner cloner = Conjunction.Cloner.cloneExactly(labelConstraints, values, isa);
            ResourceIterator<ValueConstraint<?>> valueIt = iterate(values).map(cloner::getClone).map(c -> c.asThing().asValue());
            return new Isa(cloner.conjunction(), cloner.getClone(isa).asThing().asIsa(), valueIt.toSet());
        }

        public IsaConstraint isa() {
            return isa;
        }

        @Override
        public Set<Constraint> concludableConstraints() {
            Set<Constraint> constraints = new HashSet<>();
            constraints.add(isa);
            constraints.addAll(equalsConstraints(values));
            return set(constraints);
        }

        @Override
        ResourceIterator<Unifier> unify(Rule.Conclusion conclusion, ConceptManager conceptMgr) {
            if (conclusion.isIsa()) return unify(conclusion.asIsa(), conceptMgr);
            return Iterators.empty();
        }

        ResourceIterator<Unifier> unify(Rule.Conclusion.Isa isaConclusion, ConceptManager conceptMgr) {
            Unifier.Builder unifierBuilder = Unifier.builder();
            if (unificationSatisfiable(isa().owner(), isaConclusion.isa().owner())) {
                unifierBuilder.add(isa().owner().id(), isaConclusion.isa().owner().id());
            } else return Iterators.empty();

            TypeVariable type = isa().type();
            if (unificationSatisfiable(type, isaConclusion.isa().type(), conceptMgr)) {
                unifierBuilder.add(type.id(), isaConclusion.isa().type().id());

                if (type.reference().isLabel()) {
                    // form: $r isa friendship -> require type subs(friendship) for anonymous type variable
                    unifierBuilder.requirements().types(type.id(),
                                                        subtypeLabels(type.resolvedTypes(), conceptMgr).collect(Collectors.toSet()));
                }
                addValueEqualsRequirements(unifierBuilder, values);
            } else return Iterators.empty();

            return single(unifierBuilder.build());
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
        public Variable generating() {
            return isa().owner();
        }

        @Override
        Map<Rule, Set<Unifier>> applicableRules(ConceptManager conceptMgr, LogicManager logicMgr) {
            Variable var = generating();
            Set<Label> types = var.resolvedTypes();
            // may never be empty as its always known to be at least a relation or attribute
            assert var.isSatisfiable() && !types.isEmpty();

            Map<Rule, Set<Unifier>> applicableRules = new HashMap<>();
            types.forEach(type -> logicMgr.rulesConcluding(type)
                    .forEachRemaining(rule -> unify(rule.conclusion(), conceptMgr)
                            .forEachRemaining(unifier -> {
                                applicableRules.putIfAbsent(rule, new HashSet<>());
                                applicableRules.get(rule).add(unifier);
                            })));

            return applicableRules;
        }

        @Override
        public AlphaEquivalence alphaEquals(Concludable that) {
            if (!that.isIsa()) return AlphaEquivalence.invalid();
            return isa().alphaEquals(that.asIsa().isa());
        }
    }

    /**
     * Attribute handles patterns where nothing is known about an attribute except its value:
     * `{ $a = 30; }`
     * `{ $a > 5; $a < 20; }`
     * It does not handle `{ $a < $b; }`, this scenario should be split into a Retrievable of `{ $a < $b; }`, a
     * `Concludable.Attribute` of `$a` and a `Concludable.Attribute` of `$b` (both having an empty set of
     * ValueConstraints). Only `=` is added as a requirement on the unifier.
     */
    public static class Attribute extends Concludable {

        private final Set<ValueConstraint<?>> values;
        private final ThingVariable attribute;

        private Attribute(ThingVariable attribute, Set<ValueConstraint<?>> values) {
            super(new Conjunction(set(attribute), set()));
            this.attribute = attribute;
            this.values = values;
        }

        private Attribute(IsaConstraint isa) {
            super(new Conjunction(isa.variables(), set()));
            attribute = isa.owner();
            values = set();
        }

        public static Attribute of(ThingVariable attribute) {
            return new Attribute(attribute.clone().isa(TypeVariable.of(Identifier.Variable.of(
                    Reference.label(GraqlToken.Type.ATTRIBUTE.toString()))), false));
        }

        public static Attribute of(ThingVariable attribute, Set<ValueConstraint<?>> values) {
            assert iterate(values).map(ThingConstraint::owner).toSet().equals(set(attribute));
            Conjunction.Cloner cloner = Conjunction.Cloner.cloneExactly(values);
            assert cloner.conjunction().variables().size() == 1;
            ResourceIterator<ValueConstraint<?>> valueIt = iterate(values).map(v -> cloner.getClone(v).asThing().asValue());
            return new Attribute(cloner.conjunction().variables().iterator().next().asThing(), valueIt.toSet());
        }

        @Override
        public Set<Constraint> concludableConstraints() {
            return new HashSet<>(equalsConstraints(values));
        }

        @Override
        ResourceIterator<Unifier> unify(Rule.Conclusion conclusion, ConceptManager conceptMgr) {
            if (conclusion.isValue()) return unify(conclusion.asValue());
            return Iterators.empty();
        }

        ResourceIterator<Unifier> unify(Rule.Conclusion.Value valueConclusion) {
            assert iterate(values).filter(ValueConstraint::isVariable).toSet().size() == 0;
            Unifier.Builder unifierBuilder = Unifier.builder();
            if (unificationSatisfiable(attribute, valueConclusion.value().owner())) {
                unifierBuilder.add(attribute.id(), valueConclusion.value().owner().id());
            } else return Iterators.empty();
            addValueEqualsRequirements(unifierBuilder, values);
            return single(unifierBuilder.build());
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
        public Variable generating() {
            return attribute;
        }

        @Override
        Map<Rule, Set<Unifier>> applicableRules(ConceptManager conceptMgr, LogicManager logicMgr) {
            Variable attribute = generating();
            Set<Label> attributeTypes = attribute.resolvedTypes();
            // may never be empty as its always known to be at least an attribute
            assert attribute.isSatisfiable() && !attributeTypes.isEmpty();

            Map<Rule, Set<Unifier>> applicableRules = new HashMap<>();
            attributeTypes.forEach(type -> logicMgr.rulesConcluding(type)
                    .forEachRemaining(rule -> unify(rule.conclusion(), conceptMgr)
                            .forEachRemaining(unifier -> {
                                applicableRules.putIfAbsent(rule, new HashSet<>());
                                applicableRules.get(rule).add(unifier);
                            })));

            return applicableRules;
        }

        @Override
        public AlphaEquivalence alphaEquals(Concludable that) {
            if (!that.isAttribute()) return AlphaEquivalence.invalid();
            return AlphaEquivalence.valid().validIfAlphaEqual(values, that.asAttribute().values);
        }
    }

    private static class Extractor {

        private final Set<Variable> isaOwnersToSkip = new HashSet<>();
        private final Set<Variable> valueOwnersToSkip = new HashSet<>();
        private final Set<Concludable> concludables = new HashSet<>();

        Extractor(Conjunction conjunction) {
            Set<Constraint> constraints = conjunction.variables().stream().flatMap(variable -> variable.constraints().stream())
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
            Set<LabelConstraint> labelConstraints = set(labelConstraints(relationConstraint), relationConstraint.owner().isa().map(Extractor::labelConstraints).orElse(set()));
            concludables.add(Relation.of(relationConstraint, relationConstraint.owner().isa().orElse(null), labelConstraints));
            isaOwnersToSkip.add(relationConstraint.owner());
        }

        private void fromConstraint(HasConstraint hasConstraint) {
            Set<LabelConstraint> labelConstraints = set(labelConstraints(hasConstraint), hasConstraint.attribute().isa().map(Extractor::labelConstraints).orElse(set()));
            concludables.add(Has.of(hasConstraint, hasConstraint.attribute().isa().orElse(null), hasConstraint.attribute().value(), labelConstraints));
            isaOwnersToSkip.add(hasConstraint.attribute());
            valueOwnersToSkip.add(hasConstraint.attribute());
        }

        public void fromConstraint(IsaConstraint isaConstraint) {
            if (isaOwnersToSkip.contains(isaConstraint.owner())) return;
            concludables.add(Isa.of(isaConstraint, isaConstraint.owner().value(), labelConstraints(isaConstraint)));
            isaOwnersToSkip.add(isaConstraint.owner());
            valueOwnersToSkip.add(isaConstraint.owner());
        }

        private void fromConstraint(ValueConstraint<?> valueConstraint) {
            if (valueOwnersToSkip.contains(valueConstraint.owner())) return;
            if (valueConstraint.isVariable()) {
                // form: `{ $x > $y; }`
                concludables.add(Attribute.of(valueConstraint.owner()));
                ValueConstraint.Variable val;
                if (!valueOwnersToSkip.contains((val = valueConstraint.asVariable()).value())) {
                    concludables.add(Attribute.of(val.value()));
                    valueOwnersToSkip.add(val.value());
                }
            } else {
                concludables.add(Attribute.of(valueConstraint.owner(), set(valueConstraint.owner().value())));
            }
            valueOwnersToSkip.add(valueConstraint.owner());
        }

        public Set<Concludable> concludables() {
            return set(concludables);
        }

        private static Set<LabelConstraint> labelConstraints(Constraint constraint) {
            return iterate(constraint.variables()).filter(v -> v.reference().isLabel()).map(
                    v -> v.asType().label().get()).toSet();
        }
    }
}
