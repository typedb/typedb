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

public interface Concept {

    boolean isDeleted();

    void delete();

    boolean isType();

    boolean isThingType();

    boolean isEntityType();

    boolean isAttributeType();

    boolean isRelationType();

    boolean isRoleType();

    boolean isThing();

    boolean isEntity();

    boolean isAttribute();

    boolean isRelation();

    Type asType();

    ThingType asThingType();

    EntityType asEntityType();

    AttributeType asAttributeType();

    RelationType asRelationType();

    RoleType asRoleType();

    Thing asThing();

    Entity asEntity();

    Attribute asAttribute();

    Relation asRelation();

    GraknException exception(GraknException exception);
}
