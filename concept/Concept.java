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

package grakn.core.concept;

import grakn.core.common.exception.GraknException;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.ThingType;
import grakn.core.concept.type.Type;

import static grakn.core.common.exception.Error.ThingRead.INVALID_THING_CASTING;
import static grakn.core.common.exception.Error.TypeRead.INVALID_TYPE_CASTING;

public interface Concept {

    String iid();

    boolean isDeleted();

    default Type asType() {
        throw new GraknException(INVALID_TYPE_CASTING.format(Type.class.getCanonicalName()));
    }

    default ThingType asThingType() {
        throw new GraknException(INVALID_TYPE_CASTING.format(ThingType.class.getCanonicalName()));
    }

    default EntityType asEntityType() {
        throw new GraknException(INVALID_TYPE_CASTING.format(EntityType.class.getCanonicalName()));
    }

    default AttributeType asAttributeType() {
        throw new GraknException(INVALID_TYPE_CASTING.format(AttributeType.class.getCanonicalName()));
    }

    default RelationType asRelationType() {
        throw new GraknException(INVALID_TYPE_CASTING.format(RelationType.class.getCanonicalName()));
    }

    default RoleType asRoleType() {
        throw new GraknException(INVALID_TYPE_CASTING.format(RoleType.class.getCanonicalName()));
    }

    default Thing asThing() {
        throw new GraknException(INVALID_THING_CASTING.format(Thing.class.getCanonicalName()));
    }

    default Entity asEntity() {
        throw new GraknException(INVALID_THING_CASTING.format(Entity.class.getCanonicalName()));
    }

    default Attribute asAttribute() {
        throw new GraknException(INVALID_THING_CASTING.format(Attribute.class.getCanonicalName()));
    }

    default Relation asRelation() {
        throw new GraknException(INVALID_THING_CASTING.format(Relation.class.getCanonicalName()));
    }
}
