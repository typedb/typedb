/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.kb.internal.concept;

import grakn.core.concept.Entity;
import grakn.core.concept.EntityType;
import grakn.core.concept.SchemaConcept;
import grakn.core.kb.internal.structure.VertexElement;
import grakn.core.graql.internal.Schema;

/**
 * <p>
 *     {@link SchemaConcept} used to represent categories.
 * </p>
 *
 * <p>
 *     A {@link SchemaConcept} which represents categories instances can fall within.
 *     Any instance of a {@link EntityType} is called an {@link Entity}.
 * </p>
 *
 *
 */
public class EntityTypeImpl extends TypeImpl<EntityType, Entity> implements EntityType{
    private EntityTypeImpl(VertexElement vertexElement) {
        super(vertexElement);
    }

    private EntityTypeImpl(VertexElement vertexElement, EntityType type) {
        super(vertexElement, type);
    }

    public static EntityTypeImpl get(VertexElement vertexElement){
        return new EntityTypeImpl(vertexElement);
    }

    public static EntityTypeImpl create(VertexElement vertexElement, EntityType type){
        return new EntityTypeImpl(vertexElement, type);
    }

    @Override
    public Entity create() {
        return addInstance(Schema.BaseType.ENTITY, (vertex, type) -> vertex().tx().factory().buildEntity(vertex, type), false);
    }

    public Entity addEntityInferred() {
        return addInstance(Schema.BaseType.ENTITY, (vertex, type) -> vertex().tx().factory().buildEntity(vertex, type), true);
    }

    public static EntityTypeImpl from(EntityType entityType){
        return (EntityTypeImpl) entityType;
    }
}
