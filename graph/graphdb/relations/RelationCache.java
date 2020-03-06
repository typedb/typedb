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
 */

package grakn.core.graph.graphdb.relations;

import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.HashMap;
import java.util.Map;

/**
 * Immutable map from long key ids to objects.
 * Implemented for memory and time efficiency.
 */
public class RelationCache{
    public final Direction direction;
    public final long typeId;
    public final long relationId;
    private final Object other;
    private final Map<Long, Object> properties;

    public RelationCache(Direction direction, long typeId, long relationId, Object other, Map<Long, Object> properties) {
        this.direction = direction;
        this.typeId = typeId;
        this.relationId = relationId;
        this.other = other;
        this.properties = (properties == null || properties.size() > 0) ? properties : new HashMap<>(0);
    }

    public RelationCache(Direction direction, long typeId, long relationId, Object other) {
        this(direction,typeId,relationId,other,null);
    }

    @SuppressWarnings("unchecked")
    public <O> O get(long key) {
        return (O) properties.get(key);
    }

    public boolean hasProperties() {
        return properties != null && !properties.isEmpty();
    }

    public Map<Long, Object> properties(){ return properties;}

    public Object getValue() {
        return other;
    }

    public Long getOtherVertexId() {
        return (Long) other;
    }

    @Override
    public String toString() {
         return typeId + "-" + direction + "->" + other + ":" + relationId;
    }

}
