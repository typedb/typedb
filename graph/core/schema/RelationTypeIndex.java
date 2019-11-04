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

package grakn.core.graph.core.schema;

import grakn.core.graph.core.EdgeLabel;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.RelationType;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;

/**
 * A RelationTypeIndex is an index installed on a {@link RelationType} to speed up vertex-centric indexes for that type.
 * A RelationTypeIndex is created via
 * {@link JanusGraphManagement#buildEdgeIndex(EdgeLabel, String, org.apache.tinkerpop.gremlin.structure.Direction, org.apache.tinkerpop.gremlin.process.traversal.Order, PropertyKey...)}
 * for edge labels and
 * {@link JanusGraphManagement#buildPropertyIndex(PropertyKey, String, org.apache.tinkerpop.gremlin.process.traversal.Order, PropertyKey...)}
 * for property keys.
 * <p>
 * This interface allows the inspection of already defined RelationTypeIndex'es. An existing index on a RelationType
 * can be retrieved via {@link JanusGraphManagement#getRelationIndex(RelationType, String)}.
 */
public interface RelationTypeIndex extends Index {

    /**
     * Returns the {@link RelationType} on which this index is installed.
     */
    RelationType getType();

    /**
     * Returns the sort order of this index. Index entries are sorted in this order and queries
     * which use this sort order will be faster.
     */
    Order getSortOrder();

    /**
     * Returns the (composite) sort key for this index. The composite sort key is an ordered list of {@link RelationType}s
     */
    RelationType[] getSortKey();

    /**
     * Returns the direction on which this index is installed. An index may cover only one or both directions.
     */
    Direction getDirection();

    /**
     * Returns the status of this index
     */
    SchemaStatus getIndexStatus();


}
