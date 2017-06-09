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

import ai.grakn.exception.GraphOperationException;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Optional;

/**
 * <p>
 *     Represent a Shard of a concept
 * </p>
 *
 * <p>
 *     Wraps a {@link VertexElement} which is a shard of a {@link ai.grakn.concept.Concept}.
 *     This is used to break supernodes apart. For example the instances of a {@link ai.grakn.concept.Type} are
 *     spread across several shards.
 * </p>
 *
 * @author fppt
 */
public class Shard {
    private final VertexElement vertexElement;

    public Shard(VertexElement vertexElement){
        this.vertexElement = vertexElement;
    }

    private VertexElement vertex(){
        return vertexElement;
    }

    /**
     *
     * @return The concept that this shard is part of
     */
    ConceptImpl concept(){
        Optional<Object> concept = vertex().getEdgesOfType(Direction.OUT, Schema.EdgeLabel.SHARD).
                map(EdgeElement::getTarget).findAny();
        if(concept.isPresent()) return (ConceptImpl) concept.get();
        throw GraphOperationException.unlinkedShard(vertex().id());
    }
}
