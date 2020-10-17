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
 *
 */

package grakn.core.pattern.variable;

import grakn.core.common.exception.GraknException;
import grakn.core.pattern.constraint.type.AbstractConstraint;
import grakn.core.pattern.constraint.type.LabelConstraint;
import grakn.core.pattern.constraint.type.OwnsConstraint;
import grakn.core.pattern.constraint.type.PlaysConstraint;
import grakn.core.pattern.constraint.type.RegexConstraint;
import grakn.core.pattern.constraint.type.RelatesConstraint;
import grakn.core.pattern.constraint.type.SubConstraint;
import grakn.core.pattern.constraint.type.TypeConstraint;
import grakn.core.pattern.constraint.type.ValueTypeConstraint;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class TypeVariable extends Variable {

    private LabelConstraint labelConstraint;
    private AbstractConstraint abstractConstraint;
    private ValueTypeConstraint valueTypeConstraint;
    private RegexConstraint regexConstraint;

    private final Set<SubConstraint> subConstraints;
    private final Set<OwnsConstraint> ownsConstraints;
    private final Set<PlaysConstraint> playsConstraints;
    private final Set<RelatesConstraint> relatesConstraints;
    private final Set<TypeConstraint> constraints;

    TypeVariable(final Identifier identifier) {
        super(identifier);
        this.subConstraints = new HashSet<>();
        this.ownsConstraints = new HashSet<>();
        this.playsConstraints = new HashSet<>();
        this.relatesConstraints = new HashSet<>();
        this.constraints = new HashSet<>();
    }

    TypeVariable constrain(final List<graql.lang.pattern.constraint.TypeConstraint> constraints,
                           final VariableRegistry register) {
        constraints.forEach(constraint -> this.constrain(TypeConstraint.of(this, constraint, register)));
        return this;
    }

    private void constrain(final TypeConstraint constraint) {
        constraints.add(constraint);
        if (constraint.isLabel()) labelConstraint = constraint.asLabel();
        else if (constraint.isAbstract()) abstractConstraint = constraint.asAbstract();
        else if (constraint.isValueType()) valueTypeConstraint = constraint.asValueType();
        else if (constraint.isRegex()) regexConstraint = constraint.asRegex();
        else if (constraint.isSub()) subConstraints.add(constraint.asSub());
        else if (constraint.isOwns()) ownsConstraints.add(constraint.asOwns());
        else if (constraint.isPlays()) playsConstraints.add(constraint.asPlays());
        else if (constraint.isRelates()) relatesConstraints.add(constraint.asRelates());
        else throw GraknException.of(ILLEGAL_STATE);
    }

    public Optional<LabelConstraint> label() {
        return Optional.ofNullable(labelConstraint);
    }

    public Optional<AbstractConstraint> abstractConstraint() {
        return Optional.ofNullable(abstractConstraint);
    }

    public Optional<ValueTypeConstraint> valueType() {
        return Optional.ofNullable(valueTypeConstraint);
    }

    public Optional<RegexConstraint> regex() {
        return Optional.ofNullable(regexConstraint);
    }

    public Set<SubConstraint> sub() {
        return subConstraints;
    }

    public Set<OwnsConstraint> owns() {
        return ownsConstraints;
    }

    public Set<PlaysConstraint> plays() {
        return playsConstraints;
    }

    public Set<RelatesConstraint> relates() {
        return relatesConstraints;
    }

    @Override
    public Set<TypeConstraint> constraints() {
        return constraints;
    }

    @Override
    public boolean isType() {
        return true;
    }

    @Override
    public TypeVariable asType() {
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final TypeVariable that = (TypeVariable) o;
        return this.identifier.equals(that.identifier);
    }

    @Override
    public int hashCode() {
        return identifier.hashCode();
    }
}
