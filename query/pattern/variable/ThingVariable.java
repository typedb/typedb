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
import grakn.core.query.pattern.constraint.ThingConstraint;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class ThingVariable extends Variable {

    private ThingConstraint.IID iidConstraint;
    private final Set<ThingConstraint.Isa> isaConstraints;
    private final Set<ThingConstraint.NEQ> neqConstraints;
    private final Set<ThingConstraint.Value<?>> valueConstraints;
    private final Set<ThingConstraint.Relation> relationConstraints;
    private final Set<ThingConstraint.Has> hasConstraints;
    private final Set<ThingConstraint> constraints;

    ThingVariable(final Identifier identifier) {
        super(identifier);
        this.isaConstraints = new HashSet<>();
        this.neqConstraints = new HashSet<>();
        this.valueConstraints = new HashSet<>();
        this.relationConstraints = new HashSet<>();
        this.hasConstraints = new HashSet<>();
        this.constraints = new HashSet<>();
    }

    ThingVariable constrain(final List<graql.lang.pattern.constraint.ThingConstraint> constraints,
                            final VariableRegistry register) {
        constraints.forEach(constraint -> this.constrain(ThingConstraint.of(this, constraint, register)));
        return this;
    }

    private void constrain(final ThingConstraint constraint) {
        if (constraint.isIID()) iidConstraint = constraint.asIID();
        else if (constraint.isIsa()) isaConstraints.add(constraint.asIsa());
        else if (constraint.isNEQ()) neqConstraints.add(constraint.asNEQ());
        else if (constraint.isValue()) valueConstraints.add(constraint.asValue());
        else if (constraint.isRelation()) relationConstraints.add(constraint.asRelation());
        else if (constraint.isHas()) hasConstraints.add(constraint.asHas());
        else throw GraknException.of(ILLEGAL_STATE);
    }

    public Optional<ThingConstraint.IID> iid() {
        return Optional.ofNullable(iidConstraint);
    }

    public Set<ThingConstraint.Isa> isa() {
        return isaConstraints;
    }

    public Set<ThingConstraint.NEQ> neq() {
        return neqConstraints;
    }

    public Set<ThingConstraint.Value<?>> value() {
        return valueConstraints;
    }

    public Set<ThingConstraint.Relation> relation() {
        return relationConstraints;
    }

    public Set<ThingConstraint.Has> has() {
        return hasConstraints;
    }

    @Override
    public Set<ThingConstraint> constraints() {
        return constraints;
    }

    @Override
    public boolean isThing() {
        return true;
    }

    @Override
    public ThingVariable asThing() {
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final ThingVariable that = (ThingVariable) o;
        return this.identifier.equals(that.identifier);
    }

    @Override
    public int hashCode() {
        return identifier.hashCode();
    }
}
