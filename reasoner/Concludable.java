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

package grakn.core.reasoner;

import grakn.common.collection.Pair;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Label;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.constraint.Constraint;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.Variable;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public abstract class Concludable<CONSTRAINT extends Constraint> {

    private final CONSTRAINT constraint;

    public static Set<Concludable<?>> of(Conjunction conjunction) {
        return new ConcludableRegistry(conjunction.variables()).concludables();
    }

    Concludable(CONSTRAINT constraint) {
        this.constraint = copyConstraint(constraint);
    }

    Concludable(CONSTRAINT constraint, Set<Variable> constraintContext) {
        this(constraint);
        copyAdditionalConstraints(constraintContext, new HashSet<>(this.constraint.variables()));
    }

    public CONSTRAINT constraint() {
        return constraint;
    }

    abstract CONSTRAINT copyConstraint(CONSTRAINT constraintToCopy);

    protected Stream<Unification> unifyWith(Concludable<?> other) {
        /**
         * Check the concludable types match and dispatch
         */
        if (other.isRelation()) return unifyWith(other.asRelation());
        else if (other.isHas()) return unifyWith(other.asHas());
        else if (other.isIsa()) return unifyWith(other.asIsa());
        else if (other.isValue()) return unifyWith(other.asValue());
        else throw GraknException.of(ILLEGAL_STATE);
    }

    /**
     * Find all implications that can be unified with this Concludable
     * @param allImplications All Implications stored in the DB
     * @return Implications with a `then` that could be unified with this Concludable
     */
    public Stream<Pair<Implication, Unification>> findUnifiableImplications(Stream<Implication> allImplications) {
        return allImplications.flatMap(
                implication -> implication.head().stream()
                        .flatMap(concludable -> concludable.unifyWith(this))
                        .filter(Objects::nonNull)
                        .map(unifiedBase -> new Pair<>(implication, unifiedBase)
                        ));
    }

    protected Stream<Unification> unifyWith(Concludable.Relation other) {
        return Stream.empty();
    }

    protected Stream<Unification> unifyWith(Concludable.Isa other) {
        return Stream.empty(); // TODO An empty stream isn't desirable
    }

    protected Stream<Unification> unifyWith(Concludable.Has other) {
        return Stream.empty();
    }

    protected Stream<Unification> unifyWith(Concludable.Value other) {
        return Stream.empty();
    }

    public Relation asRelation() {
        throw new GraknException(INVALID_CASTING.message(className(this.getClass()), className(Relation.class)));
    }

    public boolean isRelation() {
        return false;
    }

    public Has asHas() {
        throw new GraknException(INVALID_CASTING.message(className(this.getClass()), className(Has.class)));
    }

    public boolean isHas() {
        return false;
    }

    public Isa asIsa() {
        throw new GraknException(INVALID_CASTING.message(className(this.getClass()), className(Isa.class)));
    }

    public boolean isIsa() {
        return false;
    }

    public Value asValue() {
        throw new GraknException(INVALID_CASTING.message(className(this.getClass()), className(Value.class)));
    }

    public boolean isValue() {
        return false;
    }

    private static IsaConstraint copyIsaOntoVariable(IsaConstraint toCopy, ThingVariable variableToConstrain) {
        TypeVariable typeCopy = copyVariableWithLabelAndValueType(toCopy.type());
        IsaConstraint newIsa = variableToConstrain.isa(typeCopy, toCopy.isExplicit());
        newIsa.typeHints(toCopy.typeHints());
        return newIsa;
    }

    private static void copyValuesOntoVariable(Set<ValueConstraint<?>> toCopy, ThingVariable newOwner){
        toCopy.forEach(valueConstraint -> copyValueOntoVariable(valueConstraint, newOwner));
    }

    private static ValueConstraint<?> copyValueOntoVariable(ValueConstraint<?> toCopy, ThingVariable toConstrain) {
        ValueConstraint<?> value;
        if (toCopy.isLong()) value = toConstrain.valueLong(toCopy.asLong().predicate().asEquality(), toCopy.asLong().value());
        else if (toCopy.isDouble()) value = toConstrain.valueDouble(toCopy.asDouble().predicate().asEquality(), toCopy.asDouble().value());
        else if (toCopy.isBoolean()) value = toConstrain.valueBoolean(toCopy.asBoolean().predicate().asEquality(), toCopy.asBoolean().value());
        else if (toCopy.isString()) value = toConstrain.valueString(toCopy.asString().predicate(), toCopy.asString().value());
        else if (toCopy.isDateTime()) value = toConstrain.valueDateTime(toCopy.asDateTime().predicate().asEquality(), toCopy.asDateTime().value());
        else throw GraknException.of(ILLEGAL_STATE);
        return value;
    }

    protected static ThingVariable copyIsaAndValues(ThingVariable copyFrom) {
        ThingVariable copy = ThingVariable.of(copyFrom.identifier());
        copyIsaAndValues(copyFrom, copy);
        return copy;
    }

    private static void copyIsaAndValues(ThingVariable oldOwner, ThingVariable newOwner) {
        if (oldOwner.isa().isPresent()) copyIsaOntoVariable(oldOwner.isa().get(), newOwner);
        copyValuesOntoVariable(oldOwner.value(), newOwner);
    }

    private void copyAdditionalConstraints(Set<Variable> fromVars, Set<Variable> toVars) {
        Map<Variable, Variable> nonAnonfromVarsMap = fromVars.stream()
                .filter(variable -> !variable.identifier().reference().isAnonymous())
                .collect(Collectors.toMap(e -> e, e -> e)); // Create a map for efficient lookups
        toVars.stream().filter(variable -> !variable.identifier().reference().isAnonymous())
                .forEach(copyTo -> {
            if (nonAnonfromVarsMap.containsKey(copyTo)) {
                Variable copyFrom = nonAnonfromVarsMap.get(copyTo);
                if (copyTo.isThing() && copyFrom.isThing()) {
                    copyIsaAndValues(copyFrom.asThing(), copyTo.asThing());
                } else if (copyTo.isType() && copyFrom.isType()) {
                    copyLabelAndValueType(copyFrom.asType(), copyTo.asType());
                } else throw new GraknException(ILLEGAL_STATE);
            }
        });
    }

    private static void copyLabelAndValueType(TypeVariable copyFrom, TypeVariable copyTo) {
        if (copyFrom.label().isPresent()) copyTo.label(Label.of(copyFrom.label().get().label()));
        if (copyFrom.valueType().isPresent()) copyTo.valueType(copyFrom.valueType().get().valueType());
    }

    private static TypeVariable copyVariableWithLabelAndValueType(TypeVariable copyFrom) {
        TypeVariable copy = TypeVariable.of(copyFrom.identifier());
        copyLabelAndValueType(copyFrom, copy);
        return copy;
    }

    public static class Relation extends Concludable<RelationConstraint> {

        public Relation(RelationConstraint relationConstraint) {
            super(relationConstraint);
        }

        public Relation(RelationConstraint relationConstraint, Set<Variable> constraintContext) {
            super(relationConstraint, constraintContext);
        }

        @Override
        RelationConstraint copyConstraint(RelationConstraint relationConstraint) {
            ThingVariable ownerCopy = copyIsaAndValues(relationConstraint.owner());
            List<RelationConstraint.RolePlayer> rolePlayersCopy = copyRolePlayers(relationConstraint.players());
            return new RelationConstraint(ownerCopy, rolePlayersCopy);
        }

        static private List<RelationConstraint.RolePlayer> copyRolePlayers(List<RelationConstraint.RolePlayer> players) {
            return players.stream().map(rolePlayer -> {
                TypeVariable roleTypeCopy = rolePlayer.roleType().isPresent() ? copyVariableWithLabelAndValueType(rolePlayer.roleType().get()) : null;
                ThingVariable playerCopy = copyIsaAndValues(rolePlayer.player());
                RelationConstraint.RolePlayer rolePlayerCopy = new RelationConstraint.RolePlayer(roleTypeCopy, playerCopy);
                rolePlayerCopy.roleTypeHints(rolePlayer.roleTypeHints());
                return rolePlayerCopy;
            }).collect(Collectors.toList());
        }

        @Override
        public Concludable.Relation asRelation() {
            return this;
        }

        @Override
        public boolean isRelation() {
            return true;
        }

        @Override
        protected Stream<Unification> unifyWith(Concludable.Relation other) {
            // Check the relation variables' isa constraint labels and prune if there is no intersection

            // Find all roleplayer mapping combinations, which should have the form:
            // Set<List<Pair<RelationConstraint.RolePlayer, RelationConstraint.RolePlayer>>> rolePlayerMappings
            // For each, prune if there is no label intersection (or any other pruning, e.g. by value)
            // Then build a Unification for each valid combination
            return Stream.empty(); // TODO
        }
    }

    public static class Has extends Concludable<HasConstraint> {

        public Has(HasConstraint hasConstraint) {
            super(hasConstraint);
        }

        public Has(HasConstraint hasConstraint, Set<Variable> constraintContext) {
            super(hasConstraint, constraintContext);
        }

        HasConstraint copyConstraint(HasConstraint hasConstraint) {
            ThingVariable ownerCopy = copyIsaAndValues(hasConstraint.owner());
            ThingVariable attributeCopy = copyIsaAndValues(hasConstraint.attribute());
            return ownerCopy.has(attributeCopy);
        }

        @Override
        public Concludable.Has asHas() {
            return this;
        }

        @Override
        public boolean isHas() {
            return true;
        }

        @Override
        protected Stream<Unification> unifyWith(Concludable.Has other) {
            return null; // TODO
        }
    }

    public static class Isa extends Concludable<IsaConstraint> {

        public Isa(IsaConstraint isaConstraint) {
            super(isaConstraint);
        }

        public Isa(IsaConstraint isaConstraint, Set<Variable> constraintContext) {
            super(isaConstraint, constraintContext);
        }

        IsaConstraint copyConstraint(IsaConstraint isa) {
            ThingVariable newOwner = ThingVariable.of(isa.owner().identifier());
            copyValuesOntoVariable(isa.owner().value(), newOwner);
            return copyIsaOntoVariable(isa, newOwner);
        }

        @Override
        public Concludable.Isa asIsa() {
            return this;
        }

        @Override
        public boolean isIsa() {
            return true;
        }

        @Override
        protected Stream<Unification> unifyWith(Concludable.Isa other) {
            if (!(other.isIsa())) return Stream.empty();
            Concludable.Isa otherIsa = other.asIsa();
            Set<Label> possibleLabels = constraint().typeHints();
            Set<Label> otherLabels = otherIsa.constraint().typeHints();
            possibleLabels.retainAll(otherLabels);
            return Stream.of(new Unification(this, otherIsa, null)); // TODO Add variable mapping
        }
    }

    public static class Value extends Concludable<ValueConstraint<?>> {

        public Value(ValueConstraint<?> constraint) {
            super(constraint);
        }

        public Value(ValueConstraint<?> constraint, Set<Variable> constraintContext) {
            super(constraint, constraintContext);
        }

        ValueConstraint<?> copyConstraint(ValueConstraint<?> value) {
            ThingVariable newOwner = ThingVariable.of(value.owner().identifier());
            Set<ValueConstraint<?>> otherValues = value.owner().value().stream().filter(value1 -> value != value1)
                    .collect(Collectors.toSet());
            copyValuesOntoVariable(otherValues, newOwner);
            return copyValueOntoVariable(value, newOwner);
        }

        @Override
        public Concludable.Value asValue() {
            return this;
        }

        @Override
        public boolean isValue() {
            return true;
        }

        @Override
        protected Stream<Unification> unifyWith(Concludable.Value other) {
            return null; // TODO
        }
    }

}
