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
import org.apache.tinkerpop.gremlin.structure.Vertex;

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
class EntityImpl extends InstanceImpl<Entity, EntityType> implements Entity {
    EntityImpl(AbstractGraknGraph graknGraph, Vertex v) {
        super(graknGraph, v);
    }

    EntityImpl(AbstractGraknGraph graknGraph, Vertex v, EntityType type) {
        super(graknGraph, v, type);
    }
}