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
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.concept.type.Type;

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

    TypeDBException exception(TypeDBException exception);
}
