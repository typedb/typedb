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

package grakn.core.graph.core.schema;

import grakn.core.graph.core.RelationType;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;

/**
 * A RelationTypeIndex is an index installed on a RelationType to speed up vertex-centric indexes for that type.
 * A RelationTypeIndex is created via
 * JanusGraphManagement#buildEdgeIndex(EdgeLabel, String, org.apache.tinkerpop.gremlin.structure.Direction, org.apache.tinkerpop.gremlin.process.traversal.Order, PropertyKey...)
 * for edge labels and
 * JanusGraphManagement#buildPropertyIndex(PropertyKey, String, org.apache.tinkerpop.gremlin.process.traversal.Order, PropertyKey...)
 * for property keys.
 * <p>
 * This interface allows the inspection of already defined RelationTypeIndex'es. An existing index on a RelationType
 * can be retrieved via JanusGraphManagement#getRelationIndex(RelationType, String).
 */
public interface RelationTypeIndex extends Index {

    /**
     * Returns the RelationType on which this index is installed.
     */
    RelationType getType();

    /**
     * Returns the sort order of this index. Index entries are sorted in this order and queries
     * which use this sort order will be faster.
     */
    Order getSortOrder();

    /**
     * Returns the (composite) sort key for this index. The composite sort key is an ordered list of RelationTypes
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
