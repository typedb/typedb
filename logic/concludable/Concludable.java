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
import grakn.core.logic.tool.ConstraintCopier;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.constraint.Constraint;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.ThingConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.equivalence.AlphaEquivalence;
import grakn.core.pattern.variable.Variable;
import graql.lang.pattern.variable.Reference;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public abstract class Concludable<CONSTRAINT extends Constraint> {

    private final Map<Rule, Set<Map<Reference.Name, Set<Reference.Name>>>> applicableRules;
    private final CONSTRAINT constraint;

    private Concludable(CONSTRAINT constraint) {
        this.constraint = constraint;
        applicableRules = new HashMap<>(); // TODO Implement
    }

    public static Set<Concludable<?>> create(grakn.core.pattern.Conjunction conjunction) {
        return new Extractor(conjunction.variables()).concludables();
    }

    public Stream<Pair<Rule, Map<Reference.Name, Set<Reference.Name>>>> findUnifiableRules(Stream<Rule> allRules) {
        // TODO Get rules internally
        return allRules.flatMap(rule -> rule.possibleThenConcludables().stream()
                                               .flatMap(this::unify).map(unifiedBase -> new Pair<>(rule, unifiedBase))
        );
    }

    public CONSTRAINT constraint() {
        return constraint;
    }

    public Stream<Map<Reference.Name, Set<Reference.Name>>> getUnifiers(Rule rule) {
        return applicableRules.get(rule).stream();
    }

    public Stream<Rule> getApplicableRules() {
        return applicableRules.keySet().stream();
    }

    private Stream<Map<Reference.Name, Set<Reference.Name>>> unify(Rule.Conclusion<?> unifyWith) {
        if (unifyWith instanceof Rule.Conclusion.Relation) return unify((Rule.Conclusion.Relation) unifyWith);
        else if (unifyWith instanceof Rule.Conclusion.Has) return unify((Rule.Conclusion.Has) unifyWith);
        else if (unifyWith instanceof Rule.Conclusion.Isa) return unify((Rule.Conclusion.Isa) unifyWith);
        else if (unifyWith instanceof Rule.Conclusion.Value) return unify((Rule.Conclusion.Value) unifyWith);
        else throw GraknException.of(ILLEGAL_STATE);
    }

    Stream<Map<Reference.Name, Set<Reference.Name>>> unify(Rule.Conclusion.Relation unifyWith) {
        return Stream.empty();
    }

    Stream<Map<Reference.Name, Set<Reference.Name>>> unify(Rule.Conclusion.Has unifyWith) {
        return Stream.empty();
    }

    Stream<Map<Reference.Name, Set<Reference.Name>>> unify(Rule.Conclusion.Isa unifyWith) {
        return Stream.empty();
    }

    Stream<Map<Reference.Name, Set<Reference.Name>>> unify(Rule.Conclusion.Value unifyWith) {
        return Stream.empty();
    }

    public AlphaEquivalence alphaEquals(Concludable<?> that) {
        if (that.isRelation()) return alphaEquals(that.asRelation());
        else if (that.isHas()) return alphaEquals(that.asHas());
        else if (that.isIsa()) return alphaEquals(that.asIsa());
        else if (that.isValue()) return alphaEquals(that.asValue());
        else throw GraknException.of(ILLEGAL_STATE);
    }

    AlphaEquivalence alphaEquals(Concludable.Relation that) {
        return null;
    }

    AlphaEquivalence alphaEquals(Concludable.Has that) {
        return null;
    }

    AlphaEquivalence alphaEquals(Concludable.Isa that) {
        return null;
    }

    AlphaEquivalence alphaEquals(Concludable.Value that) {
        return null;
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

    public Conjunction conjunction() {
        return null; //TODO Make abstract and implement for all subtypes
    }

    public static class Relation extends Concludable<RelationConstraint> {

        public Relation(final RelationConstraint constraint) {
            super(ConstraintCopier.copyConstraint(constraint));
        }

        @Override
        public Stream<Map<Reference.Name, Set<Reference.Name>>> unify(Rule.Conclusion.Relation unifyWith) {
            // Check the relation variables' isa constraint labels and prune if there is no intersection

            // Find all roleplayer mapping combinations, which should have the form:
            // Set<List<Pair<RelationConstraint.RolePlayer, RelationConstraint.RolePlayer>>> rolePlayerMappings
            // For each, prune if there is no label intersection (or any other pruning, e.g. by value)
            // Then build a unifier for each valid combination
            return Stream.empty(); // TODO
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
        public Stream<Map<Reference.Name, Set<Reference.Name>>> unify(Rule.Conclusion.Has unifyWith) {
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
        public Stream<Map<Reference.Name, Set<Reference.Name>>> unify(Rule.Conclusion.Isa headIsa) {
            return null;
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
        public Stream<Map<Reference.Name, Set<Reference.Name>>> unify(Rule.Conclusion.Value unifyWith) {
            return null;
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
