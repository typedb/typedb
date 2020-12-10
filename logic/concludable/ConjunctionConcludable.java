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
import grakn.core.pattern.constraint.Constraint;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.ThingConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.variable.Variable;
import graql.lang.pattern.variable.Reference;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public abstract class ConjunctionConcludable<CONSTRAINT extends Constraint, U extends ConjunctionConcludable<CONSTRAINT, U>>
        extends Concludable<CONSTRAINT, U> {

    private ConjunctionConcludable(CONSTRAINT constraint) {
        super(constraint);
    }

    public static Set<ConjunctionConcludable<?, ?>> of(grakn.core.pattern.Conjunction conjunction) {
        return new Extractor(conjunction.variables()).concludables();
    }

    public Stream<Pair<Rule, Map<Reference, Set<Reference>>>> findUnifiableRules(Stream<Rule> allRules) {
        return allRules.flatMap(rule -> rule.head().stream()
                .flatMap(this::unify).map(unifiedBase -> new Pair<>(rule, unifiedBase))
        );
    }

    Stream<Map<Reference, Set<Reference>>> unify(HeadConcludable<?, ?> unifyWith) {
        if (unifyWith instanceof HeadConcludable.Relation) return unify((HeadConcludable.Relation) unifyWith);
        else if (unifyWith instanceof HeadConcludable.Has) return unify((HeadConcludable.Has) unifyWith);
        else if (unifyWith instanceof HeadConcludable.Isa) return unify((HeadConcludable.Isa) unifyWith);
        else if (unifyWith instanceof HeadConcludable.Value) return unify((HeadConcludable.Value) unifyWith);
        else throw GraknException.of(ILLEGAL_STATE);
    }

    Stream<Map<Reference, Set<Reference>>> unify(HeadConcludable.Relation unifyWith) {
        return Stream.empty();
    }

    Stream<Map<Reference, Set<Reference>>> unify(HeadConcludable.Has unifyWith) {
        return Stream.empty();
    }

    Stream<Map<Reference, Set<Reference>>> unify(HeadConcludable.Isa unifyWith) {
        return Stream.empty();
    }

    Stream<Map<Reference, Set<Reference>>> unify(HeadConcludable.Value unifyWith) {
        return Stream.empty();
    }


    public boolean isRelation() {
        return false;
    }

    public boolean isHas() {
        return false;
    }

    public boolean isIsa() {
        return false;
    }

    public boolean isValue() {
        return false;
    }

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

    Optional<Map<Reference, Set<Reference>>> updatedUnifier(Variable conj, Variable head, Map<Reference,
            Set<Reference>> variableMapping) {
        if (!hintsIntersect(head, conj)) return Optional.empty();
        Map<Reference, Set<Reference>> returnMapping = cloneMapping(variableMapping);
        returnMapping.putIfAbsent(conj.reference(), new HashSet<>());
        returnMapping.get(conj.reference()).add(head.reference());
        return Optional.of(returnMapping);
    }

    Map<Reference, Set<Reference>> cloneMapping(Map<Reference, Set<Reference>> mapping) {
        Map<Reference, Set<Reference>> res = new HashMap<>();
        mapping.forEach((k, v) -> res.put(k, new HashSet<>(v)));
        return res;
    }

    public static class Relation extends ConjunctionConcludable<RelationConstraint, ConjunctionConcludable.Relation> {

        public Relation(final RelationConstraint constraint) {
            super(copyConstraint(constraint));
        }

        @Override
        public Stream<Map<Reference, Set<Reference>>> unify(HeadConcludable.Relation unifyWith) {
            if (this.constraint().players().size() > unifyWith.constraint().players().size()) return Stream.empty();
            Map<Reference, Set<Reference>> unifier = new HashMap<>();
            Optional<Map<Reference, Set<Reference>>> unifierOpt;
            if (constraint.owner().reference().isName()) {
                unifierOpt = updatedUnifier(this.constraint().owner(), unifyWith.constraint().owner(), unifier);
                if (unifierOpt.isPresent()) unifier = unifierOpt.get();
                else return Stream.empty();
            }

            if (constraint().owner().isa().isPresent() && constraint.owner().isa().get().type().reference().isName()) {
                assert unifyWith.constraint().owner().isa().isPresent();
                unifierOpt = updatedUnifier(
                        this.constraint().owner().isa().get().type(),
                        unifyWith.constraint().owner().isa().get().type(),
                        unifier);
                if (unifierOpt.isPresent()) unifier = unifierOpt.get();
                else return Stream.empty();
            }

            Set<Map<Reference, Set<Reference>>> allUnifiers = new HashSet<>();
            matchRolesAndUnify(new ArrayList<>(this.constraint().players()), unifyWith.constraint().players(),
                    new HashSet<>(), unifier, allUnifiers);

            return allUnifiers.stream();
        }

        private void matchRolesAndUnify(
                List<RelationConstraint.RolePlayer> conjunctionRolePlayers,
                List<RelationConstraint.RolePlayer> headRolePlayers,
                Set<RelationConstraint.RolePlayer> visitedHeadRolePlayers,
                Map<Reference, Set<Reference>> unifier,
                Set<Map<Reference, Set<Reference>>> allUnifiers
        ) {
            if (conjunctionRolePlayers.isEmpty()) {
                allUnifiers.add(unifier);
                return;
            }
            RelationConstraint.RolePlayer conj = conjunctionRolePlayers.remove(0);
            for (RelationConstraint.RolePlayer head : headRolePlayers) {
                if (visitedHeadRolePlayers.contains(head)) continue;
                visitedHeadRolePlayers.add(head);
                updatedUnifier(conj, head, unifier).ifPresent(map -> matchRolesAndUnify(conjunctionRolePlayers, headRolePlayers,
                        visitedHeadRolePlayers, map, allUnifiers));
                visitedHeadRolePlayers.remove(head);
            }
            conjunctionRolePlayers.add(conj);
        }

        Optional<Map<Reference, Set<Reference>>> updatedUnifier(
                RelationConstraint.RolePlayer conj, RelationConstraint.RolePlayer head,
                Map<Reference, Set<Reference>> originalUnifier) {
            if (!head.roleTypeHints().isEmpty() && !conj.roleTypeHints().isEmpty() &&
                    Collections.disjoint(head.roleTypeHints(), conj.roleTypeHints())) return Optional.empty();

            Optional<Map<Reference, Set<Reference>>> unifierOpt;
            Map<Reference, Set<Reference>> unifier = cloneMapping(originalUnifier);
            if (conj.roleType().isPresent() && conj.roleType().get().reference().isName()) {
                assert head.roleType().isPresent();
                unifierOpt = updatedUnifier(conj.roleType().get(), head.roleType().get(), unifier);
                if (unifierOpt.isPresent()) unifier = unifierOpt.get();
                else return Optional.empty();
            }
            return updatedUnifier(conj.player(), head.player(), unifier);
        }

        @Override
        public boolean isRelation() {
            return true;
        }

        @Override
        public Relation asRelation() {
            return this;
        }
    }

    public static class Has extends ConjunctionConcludable<HasConstraint, ConjunctionConcludable.Has> {

        public Has(final HasConstraint constraint) {
            super(copyConstraint(constraint));
        }

        @Override
        public Stream<Map<Reference, Set<Reference>>> unify(HeadConcludable.Has unifyWith) {
            Map<Reference, Set<Reference>> unifier = new HashMap<>();
            Optional<Map<Reference, Set<Reference>>> unifierOpt;
            unifierOpt = updatedUnifier(this.constraint().owner(), unifyWith.constraint().owner(), unifier);
            if (unifierOpt.isPresent()) unifier = unifierOpt.get();
            else return Stream.empty();

            if (constraint.attribute().reference().isName()) {
                unifierOpt = updatedUnifier(this.constraint().attribute(), unifyWith.constraint().attribute(), unifier);
                if (unifierOpt.isPresent()) unifier = unifierOpt.get();
                else return Stream.empty();
            }
            return Stream.of(unifier);
        }

        @Override
        public boolean isHas() {
            return true;
        }

        @Override
        public Has asHas() {
            return this;
        }
    }

    public static class Isa extends ConjunctionConcludable<IsaConstraint, ConjunctionConcludable.Isa> {

        public Isa(final IsaConstraint constraint) {
            super(copyConstraint(constraint));
        }


        @Override
        Stream<Map<Reference, Set<Reference>>> unify(HeadConcludable.Isa unifyWith) {
            Map<Reference, Set<Reference>> unifier = new HashMap<>();
            Optional<Map<Reference, Set<Reference>>> unifierOpt;
            unifierOpt = updatedUnifier(this.constraint().owner(), unifyWith.constraint().owner(), unifier);
            if (unifierOpt.isPresent()) unifier = unifierOpt.get();
            else return Stream.empty();

            if (constraint.type().reference().isName()) {
                unifierOpt = updatedUnifier(this.constraint().type(), unifyWith.constraint().type(), unifier);
                if (unifierOpt.isPresent()) unifier = unifierOpt.get();
                else return Stream.empty();
            }
            return Stream.of(unifier);
        }

        @Override
        public boolean isIsa() {
            return true;
        }

        @Override
        public Isa asIsa() {
            return this;
        }
    }

    public static class Value extends ConjunctionConcludable<ValueConstraint<?>, ConjunctionConcludable.Value> {

        public Value(final ValueConstraint<?> constraint) {
            super(copyConstraint(constraint));
        }

        @Override
        Stream<Map<Reference, Set<Reference>>> unify(HeadConcludable.Value unifyWith) {
            Map<Reference, Set<Reference>> unifier = new HashMap<>();
            Optional<Map<Reference, Set<Reference>>> unifierOpt;
            unifierOpt = updatedUnifier(this.constraint().owner(), unifyWith.constraint().owner(), unifier);
            if (unifierOpt.isPresent()) unifier = unifierOpt.get();
            else return Stream.empty();

            if (constraint().isVariable() && constraint().asVariable().value().reference().isName()) {
                unifierOpt = updatedUnifier(this.constraint().asVariable().value(),
                        unifyWith.constraint().asVariable().value(), unifier);
                if (unifierOpt.isPresent()) unifier = unifierOpt.get();
                else return Stream.empty();
            }

            return Stream.of(unifier);
        }

        @Override
        public boolean isValue() {
            return true;
        }

        @Override
        public Value asValue() {
            return this;
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
