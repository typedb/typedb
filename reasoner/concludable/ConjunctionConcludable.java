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

package grakn.core.reasoner.concludable;

import grakn.common.collection.Pair;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Label;
import grakn.core.pattern.constraint.Constraint;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.ThingConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.variable.Variable;
import grakn.core.reasoner.Implication;
import grakn.core.reasoner.Unification;

import java.lang.ref.Reference;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

    public Stream<Pair<Implication, Unification>> findUnifiableImplications(Stream<Implication> allImplications) {
        return allImplications.flatMap(implication -> implication.head().stream()
                .flatMap(this::unify)
                .map(unifiedBase -> new Pair<>(implication, unifiedBase)
                ));
    }

    private Stream<Unification> unify(HeadConcludable<?, ?> unifyWith) {
        if (unifyWith instanceof HeadConcludable.Relation) return unify((HeadConcludable.Relation) unifyWith);
        else if (unifyWith instanceof HeadConcludable.Has) return unify((HeadConcludable.Has) unifyWith);
        else if (unifyWith instanceof HeadConcludable.Isa) return unify((HeadConcludable.Isa) unifyWith);
        else if (unifyWith instanceof HeadConcludable.Value) return unify((HeadConcludable.Value) unifyWith);
        else throw GraknException.of(ILLEGAL_STATE);
    }

    Stream<Unification> unify(HeadConcludable.Relation unifyWith) {
        return Stream.empty();
    }

    Stream<Unification> unify(HeadConcludable.Has unifyWith) {
        return Stream.empty();
    }

    Stream<Unification> unify(HeadConcludable.Isa unifyWith) {
        return Stream.empty();
    }

    Stream<Unification> unify(HeadConcludable.Value unifyWith) {
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
        throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(Relation.class)));
    }

    public Has asHas() {
        throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(Has.class)));
    }

    public Isa asIsa() {
        throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(Isa.class)));
    }

    public Value asValue() {
        throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(Value.class)));
    }

    public static class Relation extends ConjunctionConcludable<RelationConstraint, ConjunctionConcludable.Relation> {

        public Relation(final RelationConstraint constraint) {
            super(copyConstraint(constraint));
        }

        @Override
        public Stream<Map<Reference, Set<Reference>>> unify(HeadConcludable.Relation unifyWith) {
            List<RelationConstraint.RolePlayer> rolePlayers = constraint.players();
            unifyWith.constraint().players();


            // Check the relation variables' isa constraint labels and prune if there is no intersection

            // Find all roleplayer mapping combinations, which should have the form:
            // Set<List<Pair<RelationConstraint.RolePlayer, RelationConstraint.RolePlayer>>> rolePlayerMappings
            // For each, prune if there is no label intersection (or any other pruning, e.g. by value)
            // Then build a Unification for each valid combination

            return Stream.empty(); // TODO
        }

        private Set<Map<RelationConstraint.RolePlayer, RelationConstraint.RolePlayer>>  findMatches(
                 List<RelationConstraint.RolePlayer> conjunctionRolePlayers,
                 List<RelationConstraint.RolePlayer> headRolePlayers,
                 Set<Map<RelationConstraint.RolePlayer, RelationConstraint.RolePlayer>> mapping) {
            if (headRolePlayers.isEmpty()) return Collections.emptySet();
            RelationConstraint.RolePlayer head = headRolePlayers.remove(0);
            for (RelationConstraint.RolePlayer conj : conjunctionRolePlayers) {
                if (hintsIntersect(head, conj)) {
                    conjunctionRolePlayers.remove(conj);
                    Set<Map<RelationConstraint.RolePlayer, RelationConstraint.RolePlayer>> maps =
                            findMatches(headRolePlayers, conjunctionRolePlayers, mapping);
                    maps.forEach(map -> map.put(head, conj));
                    mapping.addAll(maps);
                    conjunctionRolePlayers.add(conj);
                }
            }
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
        public Stream<Unification> unify(HeadConcludable.Has unifyWith) {
            //check owners hints interact
            //map owners

            if (constraint.attribute().reference().isName()) {
                //check attribute hints interact
                //map owners
            }

            return null;
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
        public Stream<Unification> unify(HeadConcludable.Isa unifyWith) {
            if (!hintsIntersect(this.constraint().owner(), unifyWith.constraint.owner())) return Stream.empty();
            Set<Pair<Variable, Variable>> mapping = new HashSet<>();
            mapping.add(new Pair<>(this.constraint().owner(), unifyWith.constraint().owner()));
            if (constraint().type().reference().isName()) {
                if (!hintsIntersect(this.constraint().type(), unifyWith.constraint().type())) return Stream.empty();
                mapping.add(new Pair<>(this.constraint().type(), unifyWith.constraint().type()));
            }
            return Stream.of(new Unification(this, unifyWith, mapping));
        }

        boolean isConcrete() {
            return constraint.type().reference().isLabel();
        }

        boolean isAnonymous() {
            return constraint.type().reference().isAnonymous();
        }

        boolean isName() {
            return constraint.type().reference().isName();
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
        public Stream<Unification> unify(HeadConcludable.Value unifyWith) {
            if (!hintsIntersect(this.constraint().owner(), unifyWith.constraint().owner())) return Stream.empty();
            Set<Pair<Variable, Variable>> mapping = new HashSet<>();
            mapping.add(new Pair<>(this.constraint().owner(), unifyWith.constraint().owner()));
            if (constraint().isVariable() && constraint().asVariable().value().reference().isName()) {
                if (!hintsIntersect(this.constraint().asVariable().value(),
                        unifyWith.constraint().asVariable().value())) {
                    return Stream.empty();
                }
                mapping.add(new Pair<>(this.constraint().asVariable().value(),
                        unifyWith.constraint().asVariable().value()
                ));
            }
            Unification unification = new Unification(this, unifyWith, mapping);
            return Stream.of(unification);
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
