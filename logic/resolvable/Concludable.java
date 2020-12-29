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

import grakn.common.collection.Pair;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Label;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.Type;
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
import graql.lang.common.GraqlToken;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.list;
import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public abstract class Concludable<CONSTRAINT extends Constraint> extends Resolvable {

    private final Map<Rule, Set<Unifier>> applicableRules;
    private final CONSTRAINT constraint;

    private Concludable(CONSTRAINT constraint) {
        this.constraint = constraint;
        applicableRules = new HashMap<>(); // TODO Implement
    }

    public CONSTRAINT constraint() {
        return constraint;
    }

    public static Set<Concludable<?>> create(grakn.core.pattern.Conjunction conjunction) {
        return new Extractor(conjunction.variables()).concludables();
    }

    public Stream<Pair<Rule, Unifier>> findUnifiableRules(Stream<Rule> allRules, ConceptManager conceptMgr) {
        // TODO Get rules internally
        return allRules.flatMap(rule -> rule.possibleConclusions().stream()
                .flatMap(conclusion -> unify(conclusion, conceptMgr)).map(variableMapping -> new Pair<>(rule, variableMapping))
        );
    }

    public Stream<Unifier> getUnifiers(Rule rule) {
        return applicableRules.get(rule).stream();
    }

    public Stream<Rule> getApplicableRules() {
        return applicableRules.keySet().stream();
    }

    private Stream<Unifier> unify(Rule.Conclusion<?> unifyWith, ConceptManager conceptMgr) {
        if (unifyWith.isRelation()) return unify(unifyWith.asRelation(), conceptMgr);
        else if (unifyWith.isHas()) return unify(unifyWith.asHas(), conceptMgr);
        else if (unifyWith.isIsa()) return unify(unifyWith.asIsa(), conceptMgr);
        else if (unifyWith.isValue()) return unify(unifyWith.asValue(), conceptMgr);
        else throw GraknException.of(ILLEGAL_STATE);
    }

    Stream<Unifier> unify(Rule.Conclusion.Relation unifyWith, ConceptManager conceptMgr) { return Stream.empty(); }

    Stream<Unifier> unify(Rule.Conclusion.Has unifyWith, ConceptManager conceptMgr) { return Stream.empty(); }

    Stream<Unifier> unify(Rule.Conclusion.Isa unifyWith, ConceptManager conceptMgr) { return Stream.empty(); }

    Stream<Unifier> unify(Rule.Conclusion.Value unifyWith, ConceptManager conceptMgr) { return Stream.empty(); }

    public AlphaEquivalence alphaEquals(Concludable<?> that) {
        if (that.isRelation()) return alphaEquals(that.asRelation());
        else if (that.isHas()) return alphaEquals(that.asHas());
        else if (that.isIsa()) return alphaEquals(that.asIsa());
        else if (that.isValue()) return alphaEquals(that.asValue());
        else throw GraknException.of(ILLEGAL_STATE);
    }

    AlphaEquivalence alphaEquals(Concludable.Relation that) { return null; }

    AlphaEquivalence alphaEquals(Concludable.Has that) { return null; }

    AlphaEquivalence alphaEquals(Concludable.Isa that) { return null; }

    AlphaEquivalence alphaEquals(Concludable.Value that) { return null; }

    public boolean isRelation() { return false; }

    public boolean isHas() { return false; }

    public boolean isIsa() { return false; }

    public boolean isValue() { return false; }

    public Relation asRelation() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Relation.class));
    }

    public Has asHas() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Has.class));
    }

    public Isa asIsa() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Isa.class));
    }

    public Value asValue() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Value.class));
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
    boolean unificationSatisfiable(TypeVariable typeVar, TypeVariable unifyWithTypeVar, ConceptManager conceptMgr) {

        if (!typeVar.resolvedTypes().isEmpty() && !unifyWithTypeVar.resolvedTypes().isEmpty()) {
            return !Collections.disjoint(subtypeLabels(typeVar.resolvedTypes(), conceptMgr).collect(Collectors.toSet()),
                                         unifyWithTypeVar.resolvedTypes());
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
    boolean unificationSatisfiable(ThingVariable thingVar, ThingVariable unifyWithThingVar) {
        boolean satisfiable = true;
        if (!thingVar.resolvedTypes().isEmpty() && !unifyWithThingVar.resolvedTypes().isEmpty()) {
            satisfiable &= Collections.disjoint(thingVar.resolvedTypes(), unifyWithThingVar.resolvedTypes());
        }

        if (!thingVar.value().isEmpty() && !unifyWithThingVar.value().isEmpty()) {
            // TODO detect value contradictions between constant predicates
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
                    .filter(Objects::nonNull).map(RoleType::getLabel);
        } else {
            return conceptMgr.getThingType(label.name()).getSubtypes().map(Type::getLabel);
        }
    }

    public static class Relation extends Concludable<RelationConstraint> {

        public Relation(final RelationConstraint constraint) {
            super(ConstraintCopier.copyConstraint(constraint));
        }

        @Override
        public Stream<Unifier> unify(Rule.Conclusion.Relation unifyWith, ConceptManager conceptMgr) {
            if (this.constraint().players().size() > unifyWith.constraint().players().size()) return Stream.empty();
            Unifier.Builder unifierBuilder = Unifier.builder();

            if (!constraint().owner().reference().isAnonymous()) {
                assert constraint().owner().reference().isName();
                if (unificationSatisfiable(constraint().owner(), unifyWith.constraint().owner())) {
                    unifierBuilder.add(constraint().owner().identifier(), unifyWith.constraint().owner().identifier());
                } else return Stream.empty();
            }

            if (constraint().owner().isa().isPresent()) {
                assert unifyWith.constraint().owner().isa().isPresent(); // due to known shapes of rule conclusions
                TypeVariable relationType = constraint().owner().isa().get().type();
                TypeVariable unifyWithRelationType = unifyWith.constraint().owner().isa().get().type();
                if (unificationSatisfiable(relationType, unifyWithRelationType, conceptMgr)) {
                    unifierBuilder.add(relationType.identifier(), unifyWithRelationType.identifier());

                    if (relationType.reference().isLabel()) {
                        // require the unification target type variable satisfies a set of labels
                        Set<Label> allowedTypes = relationType.resolvedTypes().stream()
                                .flatMap(label -> subtypeLabels(label, conceptMgr)).collect(Collectors.toSet());
                        unifierBuilder.requirements().types(relationType.identifier(), allowedTypes);
                    }
                } else return Stream.empty();
            }

            // TODO this will work for now, but we should rewrite using role player `repetition`
            List<RolePlayer> conjRolePlayers = list(constraint().players());
            List<RolePlayer> thenRolePlayers = list(unifyWith.constraint().players());

            return matchRolePlayerIndices(conjRolePlayers, thenRolePlayers, new HashMap<>(), conceptMgr)
                    .map(indexMap -> rolePlayerMappingToUnifier(indexMap, thenRolePlayers, unifierBuilder.duplicate(), conceptMgr));
        }

        private Stream<Map<RolePlayer, Set<Integer>>> matchRolePlayerIndices(
                List<RolePlayer> conjRolePlayers, List<RolePlayer> thenRolePlayers,
                Map<RolePlayer, Set<Integer>> mapping, ConceptManager conceptMgr) {

            if (conjRolePlayers.isEmpty()) return Stream.of(mapping);
            RolePlayer conjRP = conjRolePlayers.get(0);

            return IntStream.range(0, thenRolePlayers.size())
                    .filter(thenIdx -> mapping.values().stream().noneMatch(players -> players.contains(thenIdx)))
                    .filter(thenIdx -> unificationSatisfiable(conjRP, thenRolePlayers.get(thenIdx), conceptMgr))
                    .mapToObj(thenIdx -> {
                        Map<RolePlayer, Set<Integer>> clone = cloneMapping(mapping);
                        clone.putIfAbsent(conjRP, new HashSet<>());
                        clone.get(conjRP).add(thenIdx);
                        return clone;
                    }).flatMap(newMapping -> matchRolePlayerIndices(conjRolePlayers.subList(1, conjRolePlayers.size()),
                                                                    thenRolePlayers, newMapping, conceptMgr));
        }

        private boolean unificationSatisfiable(RolePlayer rolePlayer, RolePlayer unifyWithRolePlayer, ConceptManager conceptMgr) {
            assert unifyWithRolePlayer.roleType().isPresent();
            boolean satisfiable = true;
            if (rolePlayer.roleType().isPresent()) {
                satisfiable &= unificationSatisfiable(rolePlayer.roleType().get(), unifyWithRolePlayer.roleType().get(), conceptMgr);
            }
            satisfiable &= unificationSatisfiable(rolePlayer.player(), unifyWithRolePlayer.player());
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
                                     unifierBuilder.add(roleTypeVar.identifier(), thenRP.roleType().get().identifier());

                                     if (roleTypeVar.reference().isLabel()) {
                                         Set<Label> allowedTypes = roleTypeVar.resolvedTypes().stream()
                                                 .flatMap(roleLabel -> subtypeLabels(roleLabel, conceptMgr))
                                                 .collect(Collectors.toSet());
                                         unifierBuilder.requirements().types(roleTypeVar.identifier(), allowedTypes);
                                     }
                                 }
                                 unifierBuilder.add(conjRP.player().identifier(), thenRP.player().identifier());
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
        AlphaEquivalence alphaEquals(Concludable.Relation that) {
            return constraint().alphaEquals(that.constraint());
        }
    }

    public static class Has extends Concludable<HasConstraint> {

        public Has(final HasConstraint constraint) {
            super(ConstraintCopier.copyConstraint(constraint));
        }

        @Override
        public Stream<Unifier> unify(Rule.Conclusion.Has unifyWith, ConceptManager conceptMgr) {
            Unifier.Builder unifierBuilder = Unifier.builder();
            if (unificationSatisfiable(constraint().owner(), unifyWith.constraint().owner())) {
                unifierBuilder.add(constraint().owner().identifier(), unifyWith.constraint().owner().identifier());
            } else return Stream.empty();

            ThingVariable attr = constraint().attribute();
            if (unificationSatisfiable(attr, unifyWith.constraint().attribute())) {
                unifierBuilder.add(attr.identifier(), unifyWith.constraint().attribute().identifier());
                if (attr.reference().isAnonymous()) {
                    // form: $x has age 10 -> require ISA age and PREDICATE =10
                    assert attr.isa().isPresent() && attr.isa().get().type().label().isPresent();
                    assert attr.value().size() == 1 && attr.value().iterator().next().isValueIdentity();
                    Label attrLabel = attr.isa().get().type().label().get().properLabel();
                    unifierBuilder.requirements().isaExplicit(attr.identifier(),
                                                              subtypeLabels(attrLabel, conceptMgr).collect(Collectors.toSet()));

                    // TODO enable predicates
//                    unifierBuilder.requirements().predicates(attr.identifier(), attr.value().iterator().next());
                } else if (attr.reference().isName() && attr.isa().isPresent() && attr.isa().get().type().label().isPresent()) {
                    // form: $x has age $a (may also handle $x has $a; $a isa age)   -> require ISA age
                    Label attrLabel = attr.isa().get().type().label().get().properLabel();
                    unifierBuilder.requirements().isaExplicit(attr.identifier(),
                                                              subtypeLabels(attrLabel, conceptMgr).collect(Collectors.toSet()));
                }
            } else return Stream.empty();

            return Stream.of(unifierBuilder.build());
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
        AlphaEquivalence alphaEquals(Concludable.Has that) {
            return constraint().alphaEquals(that.constraint());
        }
    }

    public static class Isa extends Concludable<IsaConstraint> {

        public Isa(final IsaConstraint constraint) {
            super(ConstraintCopier.copyConstraint(constraint));
        }

        @Override
        Stream<Unifier> unify(Rule.Conclusion.Isa unifyWith, ConceptManager conceptMgr) {
            Unifier.Builder unifierBuilder = Unifier.builder();
            if (unificationSatisfiable(constraint().owner(), unifyWith.constraint().owner())) {
                unifierBuilder.add(constraint().owner().identifier(), unifyWith.constraint().owner().identifier());
            } else return Stream.empty();

            TypeVariable type = constraint().type();
            if (unificationSatisfiable(type, unifyWith.constraint().type(), conceptMgr)) {
                unifierBuilder.add(type.identifier(), unifyWith.constraint().type().identifier());

                if (type.reference().isLabel()) {
                    // form: $r isa friendship -> require type subs(friendship) for anonymous type variable
                    unifierBuilder.requirements().types(type.identifier(),
                                                        subtypeLabels(type.resolvedTypes(), conceptMgr).collect(Collectors.toSet()));
                }
            } else return Stream.empty();

            return Stream.of(unifierBuilder.build());
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
        AlphaEquivalence alphaEquals(Concludable.Isa that) {
            return constraint().alphaEquals(that.constraint());
        }
    }

    public static class Value extends Concludable<ValueConstraint<?>> {

        public Value(final ValueConstraint<?> constraint) {
            super(ConstraintCopier.copyConstraint(constraint));
        }

        @Override
        Stream<Unifier> unify(Rule.Conclusion.Value unifyWith, ConceptManager conceptMgr) {
            Unifier.Builder unifierBuilder = Unifier.builder();
            if (unificationSatisfiable(constraint().owner(), unifyWith.constraint().owner())) {
                unifierBuilder.add(constraint().owner().identifier(), unifyWith.constraint().owner().identifier());
            } else return Stream.empty();

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
                if (unificationSatisfiable(constraint().predicate(), unifyWith.constraint().predicate())) {
                    unifierBuilder.add(value.identifier(), unifyWith.constraint().owner().identifier());
                } else return Stream.empty();
            } else {
                // form: $x > 10 -> require $x to satisfy predicate > 10
                // TODO enable
//                unifierBuilder.requirements().predicates(constraint().owner(), set(constraint()));
            }

            return Stream.of(unifierBuilder.build());
        }

        private boolean unificationSatisfiable(GraqlToken.Predicate predicate, GraqlToken.Predicate unifyWith) {
            assert unifyWith.equals(GraqlToken.Predicate.Equality.EQ);
            return !(predicate.equals(GraqlToken.Predicate.Equality.EQ) ||
                    predicate.equals(GraqlToken.Predicate.Equality.GTE) ||
                    predicate.equals(GraqlToken.Predicate.Equality.LTE));
        }

        @Override
        public boolean isValue() {
            return true;
        }

        @Override
        public Value asValue() {
            return this;
        }

        @Override
        AlphaEquivalence alphaEquals(Concludable.Value that) {
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
            concludables.add(new Concludable.Relation(relationConstraint));
            isaOwnersToSkip.add(relationConstraint.owner());
        }

        private void fromConstraint(HasConstraint hasConstraint) {
            concludables.add(new Concludable.Has(hasConstraint));
            isaOwnersToSkip.add(hasConstraint.attribute());
            if (hasConstraint.attribute().isa().isPresent()) valueOwnersToSkip.add(hasConstraint.attribute());
        }

        public void fromConstraint(IsaConstraint isaConstraint) {
            if (isaOwnersToSkip.contains(isaConstraint.owner())) return;
            concludables.add(new Concludable.Isa(isaConstraint));
            isaOwnersToSkip.add(isaConstraint.owner());
            valueOwnersToSkip.add(isaConstraint.owner());
        }

        private void fromConstraint(ValueConstraint<?> valueConstraint) {
            if (valueOwnersToSkip.contains(valueConstraint.owner())) return;
            concludables.add(new Concludable.Value(valueConstraint));
        }

        public Set<Concludable<?>> concludables() {
            return new HashSet<>(concludables);
        }
    }
}
