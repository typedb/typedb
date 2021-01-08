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
 *
 */

package grakn.core.pattern.constraint.type;

import grakn.core.common.exception.GraknException;
import grakn.core.pattern.constraint.Constraint;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.VariableCloner;
import grakn.core.pattern.variable.VariableRegistry;

import java.util.Collections;
import java.util.Set;

import static grakn.common.collection.Collections.set;
import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public abstract class TypeConstraint extends Constraint {

    final TypeVariable owner;
    private final Set<TypeVariable> variables;

    TypeConstraint(TypeVariable owner, Set<TypeVariable> additionalVariables) {
        if (owner == null) throw new NullPointerException("Null owner");
        this.owner = owner;
        variables = Collections.unmodifiableSet(set(additionalVariables, set(owner)));
    }

    public static TypeConstraint of(TypeVariable owner, graql.lang.pattern.constraint.TypeConstraint constraint,
                                    VariableRegistry registry) {
        if (constraint.isLabel()) return LabelConstraint.of(owner, constraint.asLabel());
        else if (constraint.isSub()) return SubConstraint.of(owner, constraint.asSub(), registry);
        else if (constraint.isAbstract()) return AbstractConstraint.of(owner);
        else if (constraint.isValueType()) return ValueTypeConstraint.of(owner, constraint.asValueType());
        else if (constraint.isRegex()) return RegexConstraint.of(owner, constraint.asRegex());
        else if (constraint.isOwns()) return OwnsConstraint.of(owner, constraint.asOwns(), registry);
        else if (constraint.isPlays()) return PlaysConstraint.of(owner, constraint.asPlays(), registry);
        else if (constraint.isRelates()) return RelatesConstraint.of(owner, constraint.asRelates(), registry);
        else throw GraknException.of(ILLEGAL_STATE);
    }

    public static TypeConstraint of(TypeVariable owner, graql.lang.pattern.constraint.ConceptConstraint constraint,
                                    VariableRegistry registry) {
        if (constraint.isIs()) return IsConstraint.of(owner, constraint.asIs(), registry);
        else throw GraknException.of(ILLEGAL_STATE);
    }

    public static TypeConstraint of(TypeVariable owner, TypeConstraint clone, VariableCloner cloner) {
        if (clone.isLabel()) return LabelConstraint.of(owner, clone.asLabel());
        else if (clone.isSub()) return SubConstraint.of(owner, clone.asSub(), cloner);
        else if (clone.isAbstract()) return AbstractConstraint.of(owner);
        else if (clone.isValueType()) return ValueTypeConstraint.of(owner, clone.asValueType());
        else if (clone.isRegex()) return RegexConstraint.of(owner, clone.asRegex());
        else if (clone.isOwns()) return OwnsConstraint.of(owner, clone.asOwns(), cloner);
        else if (clone.isPlays()) return PlaysConstraint.of(owner, clone.asPlays(), cloner);
        else if (clone.isRelates()) return RelatesConstraint.of(owner, clone.asRelates(), cloner);
        else if (clone.isIs()) return IsConstraint.of(owner, clone.asIs(), cloner);
        else throw GraknException.of(ILLEGAL_STATE);
    }

    @Override
    public TypeVariable owner() {
        return owner;
    }

    @Override
    public Set<TypeVariable> variables() {
        return Collections.unmodifiableSet(variables);
    }

    @Override
    public boolean isType() {
        return true;
    }

    @Override
    public TypeConstraint asType() {
        return this;
    }

    public boolean isLabel() {
        return false;
    }

    public boolean isSub() {
        return false;
    }

    public boolean isAbstract() {
        return false;
    }

    public boolean isValueType() {
        return false;
    }

    public boolean isRegex() {
        return false;
    }

    public boolean isOwns() {
        return false;
    }

    public boolean isPlays() {
        return false;
    }

    public boolean isRelates() {
        return false;
    }

    public boolean isIs() {
        return false;
    }

    public LabelConstraint asLabel() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(LabelConstraint.class));
    }

    public SubConstraint asSub() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(SubConstraint.class));
    }

    public AbstractConstraint asAbstract() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(AbstractConstraint.class));
    }

    public ValueTypeConstraint asValueType() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(ValueTypeConstraint.class));
    }

    public RegexConstraint asRegex() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(RegexConstraint.class));
    }

    public OwnsConstraint asOwns() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(OwnsConstraint.class));
    }

    public PlaysConstraint asPlays() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(PlaysConstraint.class));
    }

    public RelatesConstraint asRelates() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(RelatesConstraint.class));
    }

    public IsConstraint asIs() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(IsConstraint.class));
    }
}
