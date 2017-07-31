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

package ai.grakn.graph.internal.structure;

import ai.grakn.concept.Thing;
import ai.grakn.graph.internal.concept.ConceptImpl;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.stream.Stream;

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

    public Shard(ConceptImpl owner, VertexElement vertexElement){
        this(vertexElement);
        owner(owner);
    }

    public Shard(VertexElement vertexElement){
        this.vertexElement = vertexElement;
    }

    public VertexElement vertex(){
        return vertexElement;
    }

    /**
     *
     * @return The id of this shard. Strings are used because shards are looked up via the string index.
     */
    public String id(){
        return vertex().property(Schema.VertexProperty.ID);
    }

    /**
     *
     * @param owner Sets the owner of this shard
     */
    private void owner(ConceptImpl owner){
        vertex().putEdge(owner.vertex(), Schema.EdgeLabel.SHARD);
    }

    /**
     * Links a new concept to this shard.
     *
     * @param concept The concept to link to this shard
     */
    public void link(ConceptImpl concept){
        concept.vertex().putEdge(vertex(), Schema.EdgeLabel.ISA);
    }

    /**
     *
     * @return All the concept linked to this shard
     */
    public <V extends Thing> Stream<V> links(){
        return  vertex().getEdgesOfType(Direction.IN, Schema.EdgeLabel.ISA).
                map(EdgeElement::source).
                map(vertexElement ->  vertex().graph().factory().buildConcept(vertexElement));
    }

    /**
     *
     * @return The hash code of the underlying vertex
     */
    public int hashCode() {
        return id().hashCode();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        Shard shard = (Shard) object;

        //based on id because vertex comparisons are equivalent
        return id().equals(shard.id());
    }
}
