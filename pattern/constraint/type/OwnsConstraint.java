/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.pattern.constraint.type;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;
import com.vaticle.typedb.core.pattern.variable.VariableCloner;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;
import com.vaticle.typedb.core.traversal.GraphTraversal;
import com.vaticle.typeql.lang.common.TypeQLToken.Annotation;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeRead.OVERRIDDEN_TYPES_IN_TRAVERSAL;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.AT;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.SPACE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Constraint.AS;
import static com.vaticle.typeql.lang.common.TypeQLToken.Constraint.OWNS;

public class OwnsConstraint extends TypeConstraint {

    private final TypeVariable attributeType;
    private final TypeVariable overriddenAttributeType;
    private final Set<Annotation> annotations;
    private final int hash;

    public OwnsConstraint(TypeVariable owner, TypeVariable attributeType,
                          @Nullable TypeVariable overriddenAttributeType, Set<Annotation> annotations) {
        super(owner, attributeTypes(attributeType, overriddenAttributeType));
        this.attributeType = attributeType;
        this.overriddenAttributeType = overriddenAttributeType;
        this.annotations = annotations;
        this.hash = Objects.hash(OwnsConstraint.class, this.owner, this.attributeType,
                this.overriddenAttributeType, annotations);
        attributeType.constraining(this);
        if (overriddenAttributeType != null) overriddenAttributeType.constraining(this);
    }

    static OwnsConstraint of(TypeVariable owner, com.vaticle.typeql.lang.pattern.constraint.TypeConstraint.Owns constraint,
                             VariableRegistry registry) {
        TypeVariable attributeType = registry.registerTypeVariable(constraint.attribute());
        TypeVariable overriddenType = constraint.overridden().map(registry::registerTypeVariable).orElse(null);
        return new OwnsConstraint(owner, attributeType, overriddenType, set(constraint.annotations()));
    }

    static OwnsConstraint of(TypeVariable owner, OwnsConstraint clone, VariableCloner cloner) {
        TypeVariable attributeType = cloner.clone(clone.attribute());
        TypeVariable overriddenType = clone.overridden().map(cloner::clone).orElse(null);
        return new OwnsConstraint(owner, attributeType, overriddenType, clone.annotations());
    }

    public TypeVariable attribute() {
        return attributeType;
    }

    public Optional<TypeVariable> overridden() {
        return Optional.ofNullable(overriddenAttributeType);
    }

    public Set<Annotation> annotations() {
        return annotations;
    }

    @Override
    public void addTo(GraphTraversal.Thing traversal) {
        if (overridden().isPresent()) throw TypeDBException.of(OVERRIDDEN_TYPES_IN_TRAVERSAL);
        traversal.owns(owner.id(), attributeType.id(), annotations);
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
                this.annotations.equals(that.annotations));
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return owner.toString() + SPACE + OWNS + SPACE + attributeType.toString()
                + (overriddenAttributeType != null ? "" + SPACE + AS + SPACE + overriddenAttributeType : "")
                + (annotations.stream().map(Annotation::toString).collect(Collectors.joining(SPACE.toString(), SPACE.toString(), "")));
    }

    @Override
    public OwnsConstraint clone(Conjunction.ConstraintCloner cloner) {
        return cloner.cloneVariable(owner).owns(
                cloner.cloneVariable(attributeType),
                overriddenAttributeType == null ? null : cloner.cloneVariable(overriddenAttributeType),
                set(annotations)
        );
    }

    private static Set<TypeVariable> attributeTypes(TypeVariable attributeType, TypeVariable overriddenAttributeType) {
        return overriddenAttributeType == null
                ? set(attributeType)
                : set(attributeType, overriddenAttributeType);
    }
}
