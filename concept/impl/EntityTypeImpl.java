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

package grakn.core.concept.impl;

import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.concept.manager.ConceptNotificationChannel;
import grakn.core.kb.concept.structure.VertexElement;

/**
 * SchemaConcept used to represent categories.
 * A SchemaConcept which represents categories instances can fall within.
 * Any instance of a EntityType is called an Entity.
 */
public class EntityTypeImpl extends TypeImpl<EntityType, Entity> implements EntityType {
    public EntityTypeImpl(VertexElement vertexElement, ConceptManager conceptManager, ConceptNotificationChannel conceptNotificationChannel) {
        super(vertexElement, conceptManager, conceptNotificationChannel);
    }

    public static EntityTypeImpl from(EntityType entityType) {
        return (EntityTypeImpl) entityType;
    }

    @Override
    public Entity create() {
        return createInstance(false);
    }

    @Override
    public Entity addEntityInferred() {
        return createInstance(true);
    }

    private Entity createInstance(boolean isInferred) {
        return conceptManager.createEntity(this, isInferred);
    }

    @Override
    void trackRolePlayers() {
        conceptNotificationChannel.trackEntityInstancesRolesPlayed(this);
    }
}
