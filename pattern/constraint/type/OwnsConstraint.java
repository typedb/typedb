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
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.VariableCloner;
import grakn.core.pattern.variable.VariableRegistry;
import grakn.core.traversal.Traversal;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.TypeRead.OVERRIDDEN_TYPES_IN_TRAVERSAL;
import static graql.lang.common.GraqlToken.Char.SPACE;
import static graql.lang.common.GraqlToken.Constraint.AS;
import static graql.lang.common.GraqlToken.Constraint.IS_KEY;
import static graql.lang.common.GraqlToken.Constraint.OWNS;

public class OwnsConstraint extends TypeConstraint {

    private final TypeVariable attributeType;
    private final TypeVariable overriddenAttributeType;
    private final boolean isKey;
    private final int hash;

    public OwnsConstraint(TypeVariable owner, TypeVariable attributeType,
                          @Nullable TypeVariable overriddenAttributeType, boolean isKey) {
        super(owner, attributeTypes(attributeType, overriddenAttributeType));
        this.attributeType = attributeType;
        this.overriddenAttributeType = overriddenAttributeType;
        this.isKey = isKey;
        this.hash = Objects.hash(OwnsConstraint.class, this.owner, this.attributeType,
                                 this.overriddenAttributeType, this.isKey);
        attributeType.constraining(this);
        if (overriddenAttributeType != null) overriddenAttributeType.constraining(this);
    }

    static OwnsConstraint of(TypeVariable owner, graql.lang.pattern.constraint.TypeConstraint.Owns constraint,
                             VariableRegistry registry) {
        final TypeVariable attributeType = registry.register(constraint.attribute());
        final TypeVariable overriddenType = constraint.overridden().map(registry::register).orElse(null);
        return new OwnsConstraint(owner, attributeType, overriddenType, constraint.isKey());
    }

    static OwnsConstraint of(TypeVariable owner, OwnsConstraint clone, VariableCloner cloner) {
        final TypeVariable attributeType = cloner.clone(clone.attribute());
        final TypeVariable overriddenType = clone.overridden().map(cloner::clone).orElse(null);
        return new OwnsConstraint(owner, attributeType, overriddenType, clone.isKey());
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
    public void addTo(Traversal traversal) {
        if (overridden().isPresent()) throw GraknException.of(OVERRIDDEN_TYPES_IN_TRAVERSAL);
        traversal.owns(owner.id(), attributeType.id(), isKey);
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
    public boolean equals(Object o) {
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

    @Override
    public String toString() {
        StringBuilder syntax = new StringBuilder();
        syntax.append(OWNS).append(SPACE).append(attributeType.referenceSyntax());
        if (overriddenAttributeType != null) {
            syntax.append(SPACE).append(AS).append(SPACE).append(overriddenAttributeType.referenceSyntax());
        }
        if (isKey) syntax.append(SPACE).append(IS_KEY);

        return syntax.toString();
    }

    @Override
    public OwnsConstraint clone(Conjunction.Cloner cloner) {
        return cloner.cloneVariable(owner).owns(
                cloner.cloneVariable(attributeType),
                overriddenAttributeType == null ? null : cloner.cloneVariable(overriddenAttributeType),
                isKey
        );
    }

    private static Set<TypeVariable> attributeTypes(TypeVariable attributeType, TypeVariable overriddenAttributeType) {
        return overriddenAttributeType == null
                ? set(attributeType)
                : set(attributeType, overriddenAttributeType);
    }
}
