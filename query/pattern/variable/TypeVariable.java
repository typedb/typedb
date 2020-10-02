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

package grakn.core.query.pattern.variable;

import grakn.core.common.exception.GraknException;
import grakn.core.query.pattern.constraint.TypeConstraint;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class TypeVariable extends Variable {

    private TypeConstraint.Label labelConstraint;
    private TypeConstraint.Abstract abstractConstraint;
    private TypeConstraint.ValueType valueTypeConstraint;
    private TypeConstraint.Regex regexConstraint;
    private TypeConstraint.Then thenConstraint;
    private TypeConstraint.When whenConstraint;

    private final Set<TypeConstraint.Sub> subConstraints;
    private final Set<TypeConstraint.Owns> ownsConstraints;
    private final Set<TypeConstraint.Plays> playsConstraints;
    private final Set<TypeConstraint.Relates> relatesConstraints;
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
        else if (constraint.isThen()) thenConstraint = constraint.asThen();
        else if (constraint.isWhen()) whenConstraint = constraint.asWhen();
        else if (constraint.isSub()) subConstraints.add(constraint.asSub());
        else if (constraint.isOwns()) ownsConstraints.add(constraint.asOwns());
        else if (constraint.isPlays()) playsConstraints.add(constraint.asPlays());
        else if (constraint.isRelates()) relatesConstraints.add(constraint.asRelates());
        else throw GraknException.of(ILLEGAL_STATE);
    }

    public Optional<TypeConstraint.Label> label() {
        return Optional.ofNullable(labelConstraint);
    }

    public Optional<TypeConstraint.Abstract> abstractConstraint() {
        return Optional.ofNullable(abstractConstraint);
    }

    public Optional<TypeConstraint.ValueType> valueType() {
        return Optional.ofNullable(valueTypeConstraint);
    }

    public Optional<TypeConstraint.Regex> regex() {
        return Optional.ofNullable(regexConstraint);
    }

    public Optional<TypeConstraint.Then> then() {
        return Optional.ofNullable(thenConstraint);
    }

    public Optional<TypeConstraint.When> when() {
        return Optional.ofNullable(whenConstraint);
    }

    public Set<TypeConstraint.Sub> sub() {
        return subConstraints;
    }

    public Set<TypeConstraint.Owns> owns() {
        return ownsConstraints;
    }

    public Set<TypeConstraint.Plays> plays() {
        return playsConstraints;
    }

    public Set<TypeConstraint.Relates> relates() {
        return relatesConstraints;
    }

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
