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

/**
 * <p>
 *     An instance of Entity Type {@link EntityType}
 * </p>
 *
 * <p>
 *     This represents an entity in the graph.
 *     Entities are objects which are defined by their {@link ai.grakn.concept.Resource} and their links to
 *     other entities via {@link ai.grakn.concept.Relation}
 * </p>
 *
 * @author fppt
 */
public class EntityImpl extends ThingImpl<Entity, EntityType> implements Entity {
    EntityImpl(VertexElement vertexElement) {
        super(vertexElement);
    }

    EntityImpl(VertexElement vertexElement, EntityType type) {
        super(vertexElement, type);
    }

    public static EntityImpl from(Entity entity){
        return (EntityImpl) entity;
    }
}