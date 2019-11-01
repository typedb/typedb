// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grakn.core.graph.graphdb.relations;

import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.HashMap;
import java.util.Map;

/**
 * Immutable map from long key ids to objects.
 * Implemented for memory and time efficiency.
 *
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
