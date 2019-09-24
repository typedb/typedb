/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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
 */

package grakn.core.server.kb.concept;

import grakn.core.concept.thing.Entity;
import grakn.core.concept.type.EntityType;
import grakn.core.server.kb.Schema;
import grakn.core.server.kb.structure.VertexElement;
import grakn.core.server.session.ConceptObserver;

/**
 * SchemaConcept used to represent categories.
 * A SchemaConcept which represents categories instances can fall within.
 * Any instance of a EntityType is called an Entity.
 */
public class EntityTypeImpl extends TypeImpl<EntityType, Entity> implements EntityType {
    private EntityTypeImpl(VertexElement vertexElement, ConceptManager conceptManager, ConceptObserver conceptObserver) {
        super(vertexElement, conceptManager, conceptObserver);
    }

    private EntityTypeImpl(VertexElement vertexElement, EntityType type,
                           ConceptManager conceptManager, ConceptObserver conceptObserver) {
        super(vertexElement, type, conceptManager, conceptObserver);
    }

    public static EntityTypeImpl get(VertexElement vertexElement, ConceptManager conceptManager, ConceptObserver conceptObserver) {
        return new EntityTypeImpl(vertexElement, conceptManager, conceptObserver);
    }

    public static EntityTypeImpl create(VertexElement vertexElement, EntityType type,
                                        ConceptManager conceptManager, ConceptObserver conceptObserver) {
        return new EntityTypeImpl(vertexElement, type, conceptManager, conceptObserver);
    }

    public static EntityTypeImpl from(EntityType entityType) {
        return (EntityTypeImpl) entityType;
    }

    @Override
    public Entity create() {
        return createInstance(false);
    }

    public Entity addEntityInferred() {
        return createInstance(true);
    }

    private Entity createInstance(boolean isInferred) {
        VertexElement newInstanceVertexElement = createInstanceVertex(Schema.BaseType.ENTITY, isInferred);
        Entity instance = conceptManager.createEntity(newInstanceVertexElement, this);
        conceptObserver.entityCreated(instance, isInferred);
        return instance;
    }

    @Override
    void trackRolePlayers() {
        conceptObserver.trackEntityInstancesRolesPlayed(this);
    }
}
