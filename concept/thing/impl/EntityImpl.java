/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.concept.thing.impl;

import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.thing.Entity;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.concept.type.impl.EntityTypeImpl;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;

import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Thing.ENTITY;

public class EntityImpl extends ThingImpl implements Entity {

    private EntityImpl(ConceptManager conceptMgr, ThingVertex vertex) {
        super(conceptMgr, vertex);
        assert vertex.encoding().equals(ENTITY);
    }

    public static EntityImpl of(ConceptManager conceptMgr, ThingVertex vertex) {
        return new EntityImpl(conceptMgr, vertex);
    }

    @Override
    public EntityType getType() {
        return conceptMgr.convertEntityType(readableVertex().type()).asEntityType();
    }

    @Override
    public boolean isEntity() {
        return true;
    }

    @Override
    public EntityImpl asEntity() {
        return this;
    }
}
