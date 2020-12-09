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

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.Iterators;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.pattern.constraint.Constraint;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.ThingConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.variable.Variable;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public abstract class ThenConcludable<CONSTRAINT extends Constraint, T extends ThenConcludable<CONSTRAINT, T>>
        extends Concludable<CONSTRAINT, T> {

    private ThenConcludable(CONSTRAINT constraint, Set<Variable> constraintContext) {
        super(constraint);
        copyAdditionalConstraints(constraintContext, new HashSet<>(this.constraint.variables()));
    }

    public static ResourceIterator<? extends ThenConcludable<?, ?>> of(ThingConstraint constraint, Set<Variable> constraintContext) {
        ThenConcludable<?, ?> concludable;
        if (constraint.isRelation()) concludable = new Relation(constraint.asRelation(), constraintContext);
        else if (constraint.isHas()) concludable = new Has(constraint.asHas(), constraintContext);
        else if (constraint.isIsa()) concludable = new Isa(constraint.asIsa(), constraintContext);
        else if (constraint.isValue()) concludable = new Value(constraint.asValue(), constraintContext);
        else throw GraknException.of(ILLEGAL_STATE);
        return concludable.getGeneralisations();
    }

    abstract ResourceIterator<T> getGeneralisations();

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

    public static class Relation extends ThenConcludable<RelationConstraint, Relation> {

        public Relation(RelationConstraint constraint, Set<Variable> constraintContext) {
            super(copyConstraint(constraint), constraintContext);
        }

        @Override
        ResourceIterator<Relation> getGeneralisations() {
            // TODO
            return Iterators.iterate(Collections.singletonList(this));
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

    public static class Has extends ThenConcludable<HasConstraint, Has> {

        public Has(HasConstraint constraint, Set<Variable> constraintContext) {
            super(copyConstraint(constraint), constraintContext);
        }

        @Override
        ResourceIterator<Has> getGeneralisations() {
            // TODO
            return Iterators.iterate(Collections.singletonList(this));
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

    public static class Isa extends ThenConcludable<IsaConstraint, ThenConcludable.Isa> {

        public Isa(IsaConstraint constraint, Set<Variable> constraintContext) {
            super(copyConstraint(constraint), constraintContext);
        }

        @Override
        ResourceIterator<Isa> getGeneralisations() {
            // TODO
            return Iterators.iterate(Collections.singletonList(this));
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

    public static class Value extends ThenConcludable<ValueConstraint<?>, Value> {

        public Value(ValueConstraint<?> constraint, Set<Variable> constraintContext) {
            super(copyConstraint(constraint), constraintContext);
        }

        @Override
        ResourceIterator<Value> getGeneralisations() {
            // TODO
            return Iterators.iterate(Collections.singletonList(this));
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
}
