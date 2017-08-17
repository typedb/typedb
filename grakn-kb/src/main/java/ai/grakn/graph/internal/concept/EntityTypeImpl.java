/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graph.internal.concept;

import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.graph.internal.structure.VertexElement;
import ai.grakn.util.Schema;

/**
 * <p>
 *     Ontology element used to represent categories.
 * </p>
 *
 * <p>
 *     An ontological element which represents categories instances can fall within.
 *     Any instance of a Entity Type is called an {@link Entity}.
 * </p>
 *
 * @author fppt
 *
 */
public class EntityTypeImpl extends TypeImpl<EntityType, Entity> implements EntityType{
    EntityTypeImpl(VertexElement vertexElement) {
        super(vertexElement);
    }

    EntityTypeImpl(VertexElement vertexElement, EntityType type) {
        super(vertexElement, type);
    }

    @Override
    public Entity addEntity() {
        return addInstance(Schema.BaseType.ENTITY, (vertex, type) -> vertex().graph().factory().buildEntity(vertex, type), true);
    }
}
