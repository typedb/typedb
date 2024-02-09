/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.concept.type.impl;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.parameters.Concept.Existence;
import com.vaticle.typedb.core.common.parameters.Concept.Transitivity;
import com.vaticle.typedb.core.common.parameters.Order;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.thing.Entity;
import com.vaticle.typedb.core.concept.thing.impl.EntityImpl;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.validation.SubtypeValidation;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typeql.lang.common.TypeQLToken.Annotation;

import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeRead.TYPE_ROOT_MISMATCH;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.ROOT_TYPE_MUTATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.SCHEMA_VALIDATION_INVALID_SET_SUPERTYPE;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.Forwardable.iterateSorted;
import static com.vaticle.typedb.core.common.parameters.Concept.Existence.STORED;
import static com.vaticle.typedb.core.common.parameters.Concept.Transitivity.TRANSITIVE;
import static com.vaticle.typedb.core.common.parameters.Order.Asc.ASC;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Type.SUB;
import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Type.ENTITY_TYPE;
import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Type.Root.ENTITY;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.NEW_LINE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.SEMICOLON;

public class EntityTypeImpl extends ThingTypeImpl implements EntityType {

    public EntityTypeImpl(ConceptManager conceptMgr, TypeVertex vertex) {
        super(conceptMgr, vertex);
        if (vertex.encoding() != ENTITY_TYPE) {
            throw exception(TypeDBException.of(TYPE_ROOT_MISMATCH, vertex.label(),
                    ENTITY_TYPE.root().label(), vertex.encoding().root().label()));
        }
    }

    private EntityTypeImpl(ConceptManager conceptMgr, String label) {
        super(conceptMgr, label, ENTITY_TYPE);
        assert !label.equals(ENTITY.label());
    }

    public static EntityTypeImpl of(ConceptManager conceptMgr, String label) {
        return new EntityTypeImpl(conceptMgr, label);
    }

    @Override
    public EntityTypeImpl getSupertype() {
        return vertex.outs().edge(SUB).to().map(v -> (EntityTypeImpl) conceptMgr.convertEntityType(v)).firstOrNull();
    }

    @Override
    public Forwardable<EntityTypeImpl, Order.Asc> getSupertypes() {
        return iterateSorted(graphMgr().schema().getSupertypes(vertex), ASC)
                .filter(TypeVertex::isEntityType)
                .mapSorted(v -> (EntityTypeImpl) conceptMgr.convertEntityType(v), t -> t.vertex, ASC);
    }

    @Override
    public void setSupertype(EntityType superType) {
        validateIsNotDeleted();
        SubtypeValidation.throwIfNonEmpty(Iterators.link(
                Iterators.iterate(SubtypeValidation.Plays.validateSetSupertype(this, superType)),
                Iterators.iterate(SubtypeValidation.Owns.validateSetSupertype(this, superType))
        ).toList(), e -> exception(TypeDBException.of(SCHEMA_VALIDATION_INVALID_SET_SUPERTYPE, this.getLabel(), superType.getLabel(), e)));
        setSuperTypeVertex(((EntityTypeImpl) superType).vertex);
    }

    @Override
    public Forwardable<EntityTypeImpl, Order.Asc> getSubtypes() {
        return getSubtypes(TRANSITIVE);
    }

    @Override
    public Forwardable<EntityTypeImpl, Order.Asc> getSubtypes(Transitivity transitivity) {
        return getSubtypes(transitivity, v -> (EntityTypeImpl) conceptMgr.convertEntityType(v).asEntityType());
    }

    @Override
    public Forwardable<EntityImpl, Order.Asc> getInstances() {
        return getInstances(TRANSITIVE);
    }

    @Override
    public Forwardable<EntityImpl, Order.Asc> getInstances(Transitivity transitivity) {
        return instances(transitivity, v -> EntityImpl.of(conceptMgr, v));
    }

    @Override
    public EntityImpl create() {
        return create(STORED);
    }

    @Override
    public EntityImpl create(Existence existence) {
        validateCanHaveInstances(Entity.class);
        ThingVertex.Write instance = graphMgr().data().create(vertex, existence);
        return EntityImpl.of(conceptMgr, instance);
    }

    @Override
    public boolean isEntityType() {
        return true;
    }

    @Override
    public EntityTypeImpl asEntityType() {
        return this;
    }

    @Override
    public void getSyntax(StringBuilder builder) {
        writeSupertype(builder);
        writeAbstract(builder);
        writeOwns(builder);
        writePlays(builder);
        builder.append(SEMICOLON).append(NEW_LINE);
    }

    public static class Root extends EntityTypeImpl {

        public Root(ConceptManager conceptMgr, TypeVertex vertex) {
            super(conceptMgr, vertex);
            assert vertex.label().equals(ENTITY.label());
        }

        @Override
        public boolean isRoot() {
            return true;
        }

        @Override
        public void delete() {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setLabel(String label) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void unsetAbstract() {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public EntityTypeImpl getSupertype() {
            return null;
        }

        @Override
        public Forwardable<EntityTypeImpl, Order.Asc> getSupertypes() {
            return iterateSorted(ASC, this);
        }

        @Override
        public void setSupertype(EntityType superType) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setOwns(AttributeType attributeType, Set<Annotation> annotations) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setOwns(AttributeType attributeType, AttributeType overriddenType, Set<Annotation> annotations) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setPlays(RoleType roleType) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void setPlays(RoleType roleType, RoleType overriddenType) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }

        @Override
        public void unsetPlays(RoleType roleType) {
            throw exception(TypeDBException.of(ROOT_TYPE_MUTATION));
        }
    }
}
