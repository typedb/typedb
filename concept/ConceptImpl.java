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

package com.vaticle.typedb.core.concept;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Entity;
import com.vaticle.typedb.core.concept.thing.Relation;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.thing.impl.AttributeImpl;
import com.vaticle.typedb.core.concept.thing.impl.EntityImpl;
import com.vaticle.typedb.core.concept.thing.impl.RelationImpl;
import com.vaticle.typedb.core.concept.thing.impl.ThingImpl;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.concept.type.Type;
import com.vaticle.typedb.core.concept.type.impl.AttributeTypeImpl;
import com.vaticle.typedb.core.concept.type.impl.EntityTypeImpl;
import com.vaticle.typedb.core.concept.type.impl.RelationTypeImpl;
import com.vaticle.typedb.core.concept.type.impl.RoleTypeImpl;
import com.vaticle.typedb.core.concept.type.impl.ThingTypeImpl;
import com.vaticle.typedb.core.concept.type.impl.TypeImpl;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingRead.INVALID_THING_CASTING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeRead.INVALID_TYPE_CASTING;

public abstract class ConceptImpl implements Concept {

    @Override
    public boolean isType() {
        return false;
    }

    @Override
    public boolean isThingType() {
        return false;
    }

    @Override
    public boolean isEntityType() {
        return false;
    }

    @Override
    public boolean isAttributeType() {
        return false;
    }

    @Override
    public boolean isRelationType() {
        return false;
    }

    @Override
    public boolean isRoleType() {
        return false;
    }

    @Override
    public boolean isThing() {
        return false;
    }

    @Override
    public boolean isEntity() {
        return false;
    }

    @Override
    public boolean isAttribute() {
        return false;
    }

    @Override
    public boolean isRelation() {
        return false;
    }

    @Override
    public TypeImpl asType() {
        throw exception(TypeDBException.of(INVALID_TYPE_CASTING, className(this.getClass()), className(Type.class)));
    }

    @Override
    public ThingTypeImpl asThingType() {
        throw exception(TypeDBException.of(INVALID_TYPE_CASTING, className(this.getClass()), className(ThingType.class)));
    }

    @Override
    public EntityTypeImpl asEntityType() {
        throw exception(TypeDBException.of(INVALID_TYPE_CASTING, className(this.getClass()), className(EntityType.class)));
    }

    @Override
    public AttributeTypeImpl asAttributeType() {
        throw exception(TypeDBException.of(INVALID_TYPE_CASTING, className(this.getClass()), className(AttributeType.class)));
    }

    @Override
    public RelationTypeImpl asRelationType() {
        throw exception(TypeDBException.of(INVALID_TYPE_CASTING, className(this.getClass()), className(RelationType.class)));
    }

    @Override
    public RoleTypeImpl asRoleType() {
        throw exception(TypeDBException.of(INVALID_TYPE_CASTING, className(this.getClass()), className(RoleType.class)));
    }

    @Override
    public ThingImpl asThing() {
        throw exception(TypeDBException.of(INVALID_THING_CASTING, className(this.getClass()), className(Thing.class)));
    }

    @Override
    public EntityImpl asEntity() {
        throw exception(TypeDBException.of(INVALID_THING_CASTING, className(this.getClass()), className(Entity.class)));
    }

    @Override
    public AttributeImpl<?> asAttribute() {
        throw exception(TypeDBException.of(INVALID_THING_CASTING, className(this.getClass()), className(Attribute.class)));
    }

    @Override
    public RelationImpl asRelation() {
        throw exception(TypeDBException.of(INVALID_THING_CASTING, className(this.getClass()), className(Relation.class)));
    }

}
