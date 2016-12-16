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

package ai.grakn.graph.internal;

import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Optional;

/**
 * An ontological element which represents the categories instances can fall within.
 */
class EntityTypeImpl extends TypeImpl<EntityType, Entity> implements EntityType{
    EntityTypeImpl(AbstractGraknGraph graknGraph, Vertex v, Optional<EntityType> type) {
        super(graknGraph, v, type, Optional.empty());
    }

    @Override
    public Entity addEntity() {
        return addInstance(Schema.BaseType.ENTITY, (vertex, type) ->
                getGraknGraph().getElementFactory().buildEntity(vertex, type));
    }
}
