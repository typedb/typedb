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

package grakn.core.concept;

import grakn.core.common.exception.GraknException;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.thing.impl.AttributeImpl;
import grakn.core.concept.thing.impl.EntityImpl;
import grakn.core.concept.thing.impl.RelationImpl;
import grakn.core.concept.thing.impl.ThingImpl;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.ThingType;
import grakn.core.concept.type.Type;
import grakn.core.concept.type.impl.AttributeTypeImpl;
import grakn.core.concept.type.impl.EntityTypeImpl;
import grakn.core.concept.type.impl.RelationTypeImpl;
import grakn.core.concept.type.impl.RoleTypeImpl;
import grakn.core.concept.type.impl.ThingTypeImpl;
import grakn.core.concept.type.impl.TypeImpl;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.ThingRead.INVALID_THING_CASTING;
import static grakn.core.common.exception.ErrorMessage.TypeRead.INVALID_TYPE_CASTING;

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
        throw exception(GraknException.of(INVALID_TYPE_CASTING, className(this.getClass()), className(Type.class)));
    }

    @Override
    public ThingTypeImpl asThingType() {
        throw exception(GraknException.of(INVALID_TYPE_CASTING, className(this.getClass()), className(ThingType.class)));
    }

    @Override
    public EntityTypeImpl asEntityType() {
        throw exception(GraknException.of(INVALID_TYPE_CASTING, className(this.getClass()), className(EntityType.class)));
    }

    @Override
    public AttributeTypeImpl asAttributeType() {
        throw exception(GraknException.of(INVALID_TYPE_CASTING, className(this.getClass()), className(AttributeType.class)));
    }

    @Override
    public RelationTypeImpl asRelationType() {
        throw exception(GraknException.of(INVALID_TYPE_CASTING, className(this.getClass()), className(RelationType.class)));
    }

    @Override
    public RoleTypeImpl asRoleType() {
        throw exception(GraknException.of(INVALID_TYPE_CASTING, className(this.getClass()), className(RoleType.class)));
    }

    @Override
    public ThingImpl asThing() {
        throw exception(GraknException.of(INVALID_THING_CASTING, className(this.getClass()), className(Thing.class)));
    }

    @Override
    public EntityImpl asEntity() {
        throw exception(GraknException.of(INVALID_THING_CASTING, className(this.getClass()), className(Entity.class)));
    }

    @Override
    public AttributeImpl<?> asAttribute() {
        throw exception(GraknException.of(INVALID_THING_CASTING, className(this.getClass()), className(Attribute.class)));
    }

    @Override
    public RelationImpl asRelation() {
        throw exception(GraknException.of(INVALID_THING_CASTING, className(this.getClass()), className(Relation.class)));
    }

}
