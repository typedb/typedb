/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.pattern.constraint.type;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;
import com.vaticle.typedb.core.pattern.variable.VariableCloner;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;
import com.vaticle.typedb.core.traversal.GraphTraversal;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeRead.OVERRIDDEN_TYPES_IN_TRAVERSAL;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.SPACE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Constraint.AS;
import static com.vaticle.typeql.lang.common.TypeQLToken.Constraint.IS_KEY;
import static com.vaticle.typeql.lang.common.TypeQLToken.Constraint.OWNS;

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

    static OwnsConstraint of(TypeVariable owner, com.vaticle.typeql.lang.pattern.constraint.TypeConstraint.Owns constraint,
                             VariableRegistry registry) {
        TypeVariable attributeType = registry.register(constraint.attribute());
        TypeVariable overriddenType = constraint.overridden().map(registry::register).orElse(null);
        return new OwnsConstraint(owner, attributeType, overriddenType, constraint.isKey());
    }

    static OwnsConstraint of(TypeVariable owner, OwnsConstraint clone, VariableCloner cloner) {
        TypeVariable attributeType = cloner.clone(clone.attribute());
        TypeVariable overriddenType = clone.overridden().map(cloner::clone).orElse(null);
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
    public void addTo(GraphTraversal traversal) {
        if (overridden().isPresent()) throw TypeDBException.of(OVERRIDDEN_TYPES_IN_TRAVERSAL);
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
        OwnsConstraint that = (OwnsConstraint) o;
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
        return owner.toString() + SPACE + OWNS + SPACE + attributeType.toString()
                + (overriddenAttributeType != null ? "" + SPACE + AS + SPACE + overriddenAttributeType.toString() : "")
                + (isKey ? "" + SPACE + IS_KEY : "");
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
