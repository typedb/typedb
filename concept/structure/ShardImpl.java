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

package grakn.core.concept.structure;

import com.google.common.annotations.VisibleForTesting;
import grakn.core.core.Schema;
import grakn.core.kb.concept.structure.EdgeElement;
import grakn.core.kb.concept.structure.Shard;
import grakn.core.kb.concept.structure.VertexElement;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.stream.Stream;

/**
 * Represent a Shard of a concept
 * Wraps a VertexElement which is a shard of a Concept.
 * This is used to break supernodes apart. For example the instances of a Type are
 * spread across several shards.
 *
 * If we imagine the split between Concept and Element, Shards are designed to live on the same level
 * of access as Elements rather than Concepts. In other words, more related to the structure/implementation
 * of Grakn under the hood than higher level exposed interfaces
 */
public class ShardImpl implements Shard {
    private VertexElement vertexElement;

    public ShardImpl(VertexElement ownerVertex, VertexElement newShardVertex) {
        this(newShardVertex);
        owner(ownerVertex);
    }

    public ShardImpl(VertexElement vertexElement) {
        this.vertexElement = vertexElement;
    }

    @Override
    public VertexElement vertex() {
        return vertexElement;
    }

    /**
     * @return The id of this shard. Strings are used because shards are looked up via the string index.
     */
    @Override
    public Object id() {
        return vertex().id();
    }

    /**
     * @param ownerVertex Sets the owner of this shard
     */
    private void owner(VertexElement ownerVertex) {
        vertex().putEdge(ownerVertex, Schema.EdgeLabel.SHARD);
    }

    /**
     * Links a new concept's vertex to this shard.
     *
     * @param conceptVertex The concept to link to this shard
     */
    @Override
    public void link(VertexElement conceptVertex) {
        conceptVertex.addEdge(vertex(), Schema.EdgeLabel.ISA);
    }

    /**
     * @return All the concept linked to this shard
     */
    @Override
    @VisibleForTesting
    public Stream<VertexElement> links() {
        return vertex().getEdgesOfType(Direction.IN, Schema.EdgeLabel.ISA).map(EdgeElement::source);
    }

    /**
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
