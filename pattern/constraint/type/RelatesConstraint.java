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

package grakn.core.pattern.constraint.type;

import grakn.core.common.exception.GraknException;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.VariableRegistry;
import grakn.core.traversal.Traversal;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.TypeRead.OVERRIDDEN_TYPES_IN_TRAVERSAL;

public class RelatesConstraint extends TypeConstraint {

    private final TypeVariable roleType;
    private final TypeVariable overriddenRoleType;
    private final int hash;
    private List<Traversal> traversals;

    private RelatesConstraint(final TypeVariable owner, final TypeVariable roleType, @Nullable final TypeVariable overriddenRoleType) {
        super(owner);
        if (roleType == null) throw new NullPointerException("Null role");
        this.roleType = roleType;
        this.overriddenRoleType = overriddenRoleType;
        this.hash = Objects.hash(RelatesConstraint.class, this.owner, this.roleType, this.overriddenRoleType);
    }

    public static RelatesConstraint of(final TypeVariable owner,
                                       final graql.lang.pattern.constraint.TypeConstraint.Relates constraint,
                                       final VariableRegistry registry) {
        final TypeVariable roleType = registry.register(constraint.role());
        final TypeVariable overriddenRoleType = constraint.overridden().map(registry::register).orElse(null);
        return new RelatesConstraint(owner, roleType, overriddenRoleType);
    }

    public TypeVariable role() {
        return roleType;
    }

    public Optional<TypeVariable> overridden() {
        return Optional.ofNullable(overriddenRoleType);
    }

    @Override
    public Set<TypeVariable> variables() {
        return overriddenRoleType == null ? set(roleType) : set(roleType, overriddenRoleType);
    }

    @Override
    public List<Traversal> traversals() {
        if (overridden().isPresent()) throw GraknException.of(OVERRIDDEN_TYPES_IN_TRAVERSAL);
        if (traversals == null) traversals = list(Traversal.Path.Relates.of(owner.reference(), roleType.reference()));
        return traversals;
    }

    @Override
    public boolean isRelates() {
        return true;
    }

    @Override
    public RelatesConstraint asRelates() {
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final RelatesConstraint that = (RelatesConstraint) o;
        return (this.owner.equals(that.owner) &&
                this.roleType.equals(that.roleType) &&
                Objects.equals(this.overriddenRoleType, that.overriddenRoleType));
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
