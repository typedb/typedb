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

package grakn.core.logic.concludable;

import grakn.common.collection.Pair;
import grakn.core.common.exception.GraknException;
import grakn.core.logic.Rule;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.constraint.Constraint;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint.RolePlayer;
import grakn.core.pattern.constraint.thing.ThingConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.equivalence.AlphaEquivalence;
import grakn.core.pattern.variable.Variable;
import graql.lang.pattern.variable.Reference;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public abstract class ConjunctionConcludable<CONSTRAINT extends Constraint, U extends ConjunctionConcludable<CONSTRAINT, U>>
        extends Concludable<CONSTRAINT, U> {

    private final Map<Rule, Set<Map<Reference.Name, Set<Reference.Name>>>> applicableRules;

    private ConjunctionConcludable(CONSTRAINT constraint) {
        super(constraint);
        applicableRules = new HashMap<>(); // TODO Implement
    }

    public static Set<ConjunctionConcludable<?, ?>> create(grakn.core.pattern.Conjunction conjunction) {
        return new Extractor(conjunction.variables()).concludables();
    }

    public Stream<Pair<Rule, Map<Reference, Set<Reference>>>> findUnifiableRules(Stream<Rule> allRules) {
        // TODO Get rules internally
        return allRules.flatMap(rule -> rule.possibleThenConcludables().stream()
                .flatMap(this::unify).map(variableMapping -> new Pair<>(rule, variableMapping))
        );
    }

    public Stream<Map<Reference.Name, Set<Reference.Name>>> getUnifiers(Rule rule) {
        return applicableRules.get(rule).stream();
    }

    public Stream<Rule> getApplicableRules() {
        return applicableRules.keySet().stream();
    }

    private Stream<Map<Reference, Set<Reference>>> unify (ThenConcludable < ?, ?>unifyWith){
            if (unifyWith instanceof ThenConcludable.Relation) return unify((ThenConcludable.Relation) unifyWith);
            else if (unifyWith instanceof ThenConcludable.Has) return unify((ThenConcludable.Has) unifyWith);
            else if (unifyWith instanceof ThenConcludable.Isa) return unify((ThenConcludable.Isa) unifyWith);
            else if (unifyWith instanceof ThenConcludable.Value) return unify((ThenConcludable.Value) unifyWith);
            else throw GraknException.of(ILLEGAL_STATE);
        }

        Stream<Map<Reference, Set<Reference>>> unify (ThenConcludable.Relation unifyWith){
            return Stream.empty();
        }

        Stream<Map<Reference, Set<Reference>>> unify (ThenConcludable.Has unifyWith){
            return Stream.empty();
        }

        Stream<Map<Reference, Set<Reference>>> unify (ThenConcludable.Isa unifyWith){
            return Stream.empty();
        }

        Stream<Map<Reference, Set<Reference>>> unify (ThenConcludable.Value unifyWith){
            return Stream.empty();
        }

        public AlphaEquivalence alphaEquals (ConjunctionConcludable < ?, ?> that){
            if (that.isRelation()) return alphaEquals(that.asRelation());
            else if (that.isHas()) return alphaEquals(that.asHas());
            else if (that.isIsa()) return alphaEquals(that.asIsa());
            else if (that.isValue()) return alphaEquals(that.asValue());
            else throw GraknException.of(ILLEGAL_STATE);
        }

        AlphaEquivalence alphaEquals (ConjunctionConcludable.Relation that){
            return null;
        }

        AlphaEquivalence alphaEquals (ConjunctionConcludable.Has that){
            return null;
        }

        AlphaEquivalence alphaEquals (ConjunctionConcludable.Isa that){
            return null;
        }

        AlphaEquivalence alphaEquals (ConjunctionConcludable.Value that){
            return null;
        }

        public boolean isRelation () {
            return false;
        }

        public boolean isHas () {
            return false;
        }

        public boolean isIsa () {
            return false;
        }

        public boolean isValue () {
            return false;
        }

        public Relation asRelation () {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Relation.class));
        }

        public Has asHas () {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Has.class));
        }

        public Isa asIsa () {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Isa.class));
        }

        public Value asValue () {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Value.class));
        }

        Optional<Map<Reference, Set<Reference>>> tryExtendUnifier (
                Variable conjVar, Variable headVar, Map < Reference, Set < Reference >> variableMapping){
            if (varHintsDisjoint(headVar, conjVar)) return Optional.empty();
            Map<Reference, Set<Reference>> cloneOfMapping = cloneMapping(variableMapping);
            cloneOfMapping.putIfAbsent(conjVar.reference(), new HashSet<>());
            cloneOfMapping.get(conjVar.reference()).add(headVar.reference());
            return Optional.of(cloneOfMapping);
        }

    <T, V > Map < T, Set < V >> cloneMapping(Map < T, Set < V >> mapping) {
            Map<T, Set<V>> clone = new HashMap<>();
            mapping.forEach((key, set) -> clone.put(key, new HashSet<>(set)));
            return clone;
        }

        public Conjunction conjunction () {
            return null; //TODO Make abstract and implement for all subtypes
        }

        public static class Relation extends ConjunctionConcludable<RelationConstraint, ConjunctionConcludable.Relation> {

            public Relation(final RelationConstraint constraint) {
                super(copyConstraint(constraint));
            }

            @Override
            public Stream<Map<Reference, Set<Reference>>> unify(ThenConcludable.Relation unifyWith) {
                if (this.constraint().players().size() > unifyWith.constraint().players().size()) return Stream.empty();
                Map<Reference, Set<Reference>> variableUnifier = new HashMap<>();
                Optional<Map<Reference, Set<Reference>>> newUnifier;
                if (constraint.owner().reference().isName()) {
                    newUnifier = tryExtendUnifier(this.constraint().owner(), unifyWith.constraint().owner(), variableUnifier);
                    if (newUnifier.isPresent()) variableUnifier = newUnifier.get();
                    else return Stream.empty();
                }

                if (constraint.owner().isa().isPresent() && constraint.owner().isa().get().type().reference().isName()) {
                    assert unifyWith.constraint().owner().isa().isPresent();
                    newUnifier = tryExtendUnifier(this.constraint().owner().isa().get().type(),
                                                  unifyWith.constraint().owner().isa().get().type(), variableUnifier);
                    if (newUnifier.isPresent()) variableUnifier = newUnifier.get();
                    else return Stream.empty();
                }

                Map<Reference, Set<Reference>> baseVariableUnifier = variableUnifier;
                List<RolePlayer> conjRolePlayers = Collections.unmodifiableList(constraint.players());
                List<RolePlayer> thenRolePlayers = Collections.unmodifiableList(unifyWith.constraint().players());

                return matchRolePlayerIndices(conjRolePlayers, thenRolePlayers, new HashMap<>())
                        .map(indexMap -> mapRolePlayerMatchingToUnifier(indexMap, thenRolePlayers, baseVariableUnifier));
            }

            private Stream<Map<RolePlayer, Set<Integer>>> matchRolePlayerIndices(
                    List<RolePlayer> conjRolePlayers, List<RolePlayer> thenRolePlayers,
                    Map<RolePlayer, Set<Integer>> unifier) {

                if (conjRolePlayers.isEmpty()) return Stream.of(unifier);
                RolePlayer conjRP = conjRolePlayers.get(0);

                return IntStream.range(0, thenRolePlayers.size())
                        .filter(thenIdx -> unifier.values().stream().noneMatch(players -> players.contains(thenIdx)))
                        .mapToObj(thenIdx ->
                                          tryExtendRolePlayerUnifier(conjRP, thenRolePlayers.get(thenIdx), thenIdx, unifier))
                        .filter(Optional::isPresent).map(Optional::get)
                        .flatMap(newUnifier -> matchRolePlayerIndices(conjRolePlayers.subList(1, conjRolePlayers.size()),
                                                                      thenRolePlayers, newUnifier));
            }

            private Map<Reference, Set<Reference>> mapRolePlayerMatchingToUnifier(
                    Map<RolePlayer, Set<Integer>> mathcedRolePlayerIndices, List<RolePlayer> thenRolePlayers,
                    Map<Reference, Set<Reference>> variableUnifier) {
                Map<Reference, Set<Reference>> newUnifier = cloneMapping(variableUnifier);
                mathcedRolePlayerIndices.forEach((conjRP, thenRPIndices) -> thenRPIndices.stream().map(thenRolePlayers::get)
                        .forEach(thenRP -> {
                                     if (conjRP.roleType().isPresent() && conjRP.roleType().get().reference().isName()) {
                                         assert thenRP.roleType().isPresent();
                                         newUnifier.putIfAbsent(conjRP.roleType().get().reference(), new HashSet<>());
                                         newUnifier.get(conjRP.roleType().get().reference()).add(thenRP.roleType().get().reference());
                                     }
                                     newUnifier.putIfAbsent(conjRP.player().reference(), new HashSet<>());
                                     newUnifier.get(conjRP.player().reference()).add(thenRP.player().reference());
                                 }
                        ));

                return newUnifier;
            }

            private boolean roleHintsDisjoint(RolePlayer conjRP, RolePlayer thenRP) {
                if (!conjRP.roleTypeHints().isEmpty() && !thenRP.roleTypeHints().isEmpty()
                        && Collections.disjoint(conjRP.roleTypeHints(), thenRP.roleTypeHints())) return true;
                if (varHintsDisjoint(conjRP.player(), thenRP.player())) return true;
                if (conjRP.roleType().isPresent()) {
                    assert thenRP.roleType().isPresent();
                    return varHintsDisjoint(conjRP.roleType().get(), thenRP.roleType().get());
                }
                return false;
            }

            private Optional<Map<RolePlayer, Set<Integer>>> tryExtendRolePlayerUnifier(
                    RolePlayer conjRP, RolePlayer thenRP, int thenIdx, Map<RolePlayer, Set<Integer>> originalUnifier) {
                if (roleHintsDisjoint(conjRP, thenRP)) return Optional.empty();
                Map<RolePlayer, Set<Integer>> newUnifier = cloneMapping(originalUnifier);
                newUnifier.putIfAbsent(conjRP, new HashSet<>());
                newUnifier.get(conjRP).add(thenIdx);
                return Optional.of(newUnifier);
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
            AlphaEquivalence alphaEquals(ConjunctionConcludable.Relation that) {
                return constraint.alphaEquals(that.constraint());
            }
        }

        public static class Has extends ConjunctionConcludable<HasConstraint, ConjunctionConcludable.Has> {

            public Has(final HasConstraint constraint) {
                super(copyConstraint(constraint));
            }

            @Override
            public Stream<Map<Reference, Set<Reference>>> unify(ThenConcludable.Has unifyWith) {
                Optional<Map<Reference, Set<Reference>>> unifier = tryExtendUnifier(constraint.owner(),
                                                                                    unifyWith.constraint().owner(), new HashMap<>());
                if (!unifier.isPresent()) return Stream.empty();
                if (constraint.attribute().reference().isName()) {
                    unifier = tryExtendUnifier(constraint.attribute(), unifyWith.constraint().attribute(), unifier.get());
                    if (!unifier.isPresent()) return Stream.empty();
                }
                return Stream.of(unifier.get());
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
            AlphaEquivalence alphaEquals(ConjunctionConcludable.Has that) {
                return constraint.alphaEquals(that.constraint());
            }
        }

        public static class Isa extends ConjunctionConcludable<IsaConstraint, ConjunctionConcludable.Isa> {

            public Isa(final IsaConstraint constraint) {
                super(copyConstraint(constraint));
            }

            @Override
            Stream<Map<Reference, Set<Reference>>> unify(ThenConcludable.Isa unifyWith) {
                Optional<Map<Reference, Set<Reference>>> unifier = tryExtendUnifier(constraint.owner(),
                                                                                    unifyWith.constraint().owner(), new HashMap<>());
                if (!unifier.isPresent()) return Stream.empty();
                if (constraint.type().reference().isName()) {
                    unifier = tryExtendUnifier(constraint.type(), unifyWith.constraint().type(), unifier.get());
                    if (!unifier.isPresent()) return Stream.empty();
                }
                return Stream.of(unifier.get());
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
            AlphaEquivalence alphaEquals(ConjunctionConcludable.Isa that) {
                return constraint.alphaEquals(that.constraint());
            }
        }

        public static class Value extends ConjunctionConcludable<ValueConstraint<?>, ConjunctionConcludable.Value> {

            public Value(final ValueConstraint<?> constraint) {
                super(copyConstraint(constraint));
            }

            @Override
            Stream<Map<Reference, Set<Reference>>> unify(ThenConcludable.Value unifyWith) {
                Optional<Map<Reference, Set<Reference>>> unifier = tryExtendUnifier(constraint.owner(),
                                                                                    unifyWith.constraint().owner(), new HashMap<>());
                if (!unifier.isPresent()) return Stream.empty();
                if (constraint.isVariable() && constraint.asVariable().value().reference().isName()) {
                    unifier = tryExtendUnifier(constraint.asVariable().value(),
                                               unifyWith.constraint().asVariable().value(), unifier.get());
                    if (!unifier.isPresent()) return Stream.empty();
                }
                return Stream.of(unifier.get());
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
            AlphaEquivalence alphaEquals(ConjunctionConcludable.Value that) {
                return constraint.alphaEquals(that.constraint());
            }
        }


        private static class Extractor {

            private final Set<Variable> isaOwnersToSkip = new HashSet<>();
            private final Set<Variable> valueOwnersToSkip = new HashSet<>();
            private final Set<ConjunctionConcludable<?, ?>> concludables = new HashSet<>();

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
                concludables.add(new ConjunctionConcludable.Relation(relationConstraint));
                isaOwnersToSkip.add(relationConstraint.owner());
            }

            private void fromConstraint(HasConstraint hasConstraint) {
                concludables.add(new ConjunctionConcludable.Has(hasConstraint));
                isaOwnersToSkip.add(hasConstraint.attribute());
                if (hasConstraint.attribute().isa().isPresent()) valueOwnersToSkip.add(hasConstraint.attribute());
            }

            public void fromConstraint(IsaConstraint isaConstraint) {
                if (isaOwnersToSkip.contains(isaConstraint.owner())) return;
                concludables.add(new ConjunctionConcludable.Isa(isaConstraint));
                isaOwnersToSkip.add(isaConstraint.owner());
                valueOwnersToSkip.add(isaConstraint.owner());
            }

            private void fromConstraint(ValueConstraint<?> valueConstraint) {
                if (valueOwnersToSkip.contains(valueConstraint.owner())) return;
                concludables.add(new ConjunctionConcludable.Value(valueConstraint));
            }

            public Set<ConjunctionConcludable<?, ?>> concludables() {
                return new HashSet<>(concludables);
            }
        }
    }
