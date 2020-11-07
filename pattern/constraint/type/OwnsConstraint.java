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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.TypeRead.OVERRIDDEN_TYPES_IN_TRAVERSAL;

public class OwnsConstraint extends TypeConstraint {

    private final TypeVariable attributeType;
    private final TypeVariable overriddenAttributeType;
    private final boolean isKey;
    private final int hash;

    private OwnsConstraint(final TypeVariable owner, final TypeVariable attributeType,
                           @Nullable final TypeVariable overriddenAttributeType, final boolean isKey) {
        super(owner);
        this.attributeType = attributeType;
        this.overriddenAttributeType = overriddenAttributeType;
        this.isKey = isKey;
        this.hash = Objects.hash(OwnsConstraint.class, this.owner, this.attributeType, this.overriddenAttributeType, this.isKey);
    }

    public static OwnsConstraint of(final TypeVariable owner,
                                    final graql.lang.pattern.constraint.TypeConstraint.Owns constraint,
                                    final VariableRegistry registry) {
        final TypeVariable attributeType = registry.register(constraint.attribute());
        final TypeVariable overriddenType = constraint.overridden().map(registry::register).orElse(null);
        return new OwnsConstraint(owner, attributeType, overriddenType, constraint.isKey());
    }

    public TypeVariable attribute() {
        return attributeType;
    }

    public Optional<TypeVariable> overridden() {
        return Optional.ofNullable(overriddenAttributeType);
    }

    public boolean isKey() {
        return isKey;
    }

    @Override
    public Set<TypeVariable> variables() {
        return overriddenAttributeType == null
                ? set(attributeType)
                : set(attributeType, overriddenAttributeType);
    }

    @Override
    public void addTo(final Traversal traversal) {
        if (overridden().isPresent()) throw GraknException.of(OVERRIDDEN_TYPES_IN_TRAVERSAL);
        traversal.owns(owner.identifier(), attributeType.identifier());
    }

    @Override
    public boolean isOwns() {
        return true;
    }

    @Override
    public OwnsConstraint asOwns() {
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final OwnsConstraint that = (OwnsConstraint) o;
        return (this.owner.equals(that.owner) &&
                this.attributeType.equals(that.attributeType) &&
                Objects.equals(this.overriddenAttributeType, that.overriddenAttributeType) &&
                this.isKey == that.isKey);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
