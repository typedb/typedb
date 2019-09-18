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
import grakn.core.server.session.cache.TransactionCache;

/**
 * SchemaConcept used to represent categories.
 * A SchemaConcept which represents categories instances can fall within.
 * Any instance of a EntityType is called an Entity.
 */
public class EntityTypeImpl extends TypeImpl<EntityType, Entity> implements EntityType {
    private EntityTypeImpl(VertexElement vertexElement, ConceptFactory conceptFactory, TransactionCache transactionCache) {
        super(vertexElement, conceptFactory, transactionCache);
    }

    private EntityTypeImpl(VertexElement vertexElement, EntityType type,
                           ConceptFactory conceptFactory, TransactionCache transactionCache) {
        super(vertexElement, type, conceptFactory, transactionCache);
    }

    public static EntityTypeImpl get(VertexElement vertexElement, ConceptFactory conceptFactory, TransactionCache transactionCache) {
        return new EntityTypeImpl(vertexElement, conceptFactory, transactionCache);
    }

    public static EntityTypeImpl create(VertexElement vertexElement, EntityType type,
                                        ConceptFactory conceptFactory, TransactionCache transactionCache) {
        return new EntityTypeImpl(vertexElement, type, conceptFactory, transactionCache);
    }

    public static EntityTypeImpl from(EntityType entityType) {
        return (EntityTypeImpl) entityType;
    }

    @Override
    public Entity create() {
        return addInstance(Schema.BaseType.ENTITY, (vertex, type) -> conceptFactory.buildEntity(vertex, type), false);
    }

    public Entity addEntityInferred() {
        return addInstance(Schema.BaseType.ENTITY, (vertex, type) -> conceptFactory.buildEntity(vertex, type), true);
    }
}
