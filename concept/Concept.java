/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.concept;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.value.Value;
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

    boolean isValue();

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

    Value<?> asValue();

    TypeDBException exception(TypeDBException exception);

    interface Readable {

        String KEY_TYPE = "type";
        String KEY_ROOT = "root";
        String KEY_LABEL = "label";
        String KEY_VALUE = "value";
        String KEY_VALUE_TYPE = "value_type";

        boolean isType();

        boolean isAttribute();

        boolean isValue();

        Type asType();

        Attribute asAttribute();

        Value<?> asValue();

    }
}
