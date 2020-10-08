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

package grakn.core.query.pattern.constraint.type;

import grakn.core.query.pattern.variable.TypeVariable;
import grakn.core.query.pattern.variable.VariableRegistry;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class PlaysConstraint extends TypeConstraint {

    private final TypeVariable relationType;
    private final TypeVariable roleType;
    private final TypeVariable overriddenRoleType;
    private final int hash;

    private PlaysConstraint(final TypeVariable owner, @Nullable final TypeVariable relationType,
                            final TypeVariable roleType, @Nullable final TypeVariable overriddenRoleType) {
        super(owner);
        if (roleType == null) throw new NullPointerException("Null role");
        this.relationType = relationType;
        this.roleType = roleType;
        this.overriddenRoleType = overriddenRoleType;
        this.hash = Objects.hash(PlaysConstraint.class, this.owner, this.relationType, this.roleType, this.overriddenRoleType);
    }

    public static PlaysConstraint of(final TypeVariable owner,
                                     final graql.lang.pattern.constraint.TypeConstraint.Plays constraint,
                                     final VariableRegistry registry) {
        final TypeVariable roleType = registry.register(constraint.role());
        final TypeVariable relationType = constraint.relation().map(registry::register).orElse(null);
        final TypeVariable overriddenType = constraint.overridden().map(registry::register).orElse(null);
        return new PlaysConstraint(owner, relationType, roleType, overriddenType);
    }

    public Optional<TypeVariable> relation() {
        return Optional.ofNullable(relationType);
    }

    public TypeVariable role() {
        return roleType;
    }

    public Optional<TypeVariable> overridden() {
        return Optional.ofNullable(overriddenRoleType);
    }

    @Override
    public Set<TypeVariable> variables() {
        final Set<TypeVariable> variables = new HashSet<>();
        variables.add(roleType);
        if (relationType != null) variables.add(relationType);
        if (overriddenRoleType != null) variables.add(overriddenRoleType);
        return variables;
    }

    @Override
    public boolean isPlays() {
        return true;
    }

    @Override
    public PlaysConstraint asPlays() {
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final PlaysConstraint that = (PlaysConstraint) o;
        return (this.owner.equals(that.owner) &&
                this.roleType.equals(that.roleType) &&
                Objects.equals(this.relationType, that.relationType) &&
                Objects.equals(this.overriddenRoleType, that.overriddenRoleType));
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
