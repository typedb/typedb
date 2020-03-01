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

import grakn.core.graph.core.PropertyKey;
import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * A JanusGraphIndex is an index installed on the graph in order to be able to efficiently retrieve graph elements
 * by their properties.
 * A JanusGraphIndex may either be a composite or mixed index and is created via JanusGraphManagement#buildIndex(String, Class).
 * <p>
 * This interface allows introspecting an existing graph index. Existing graph indexes can be retrieved via
 * JanusGraphManagement#getGraphIndex(String) or JanusGraphManagement#getGraphIndexes(Class).
 *
 */
public interface JanusGraphIndex extends Index {

    /**
     * Returns the name of the index
     * @return
     */
    String name();

    /**
     * Returns the name of the backing index. For composite indexes this returns a default name.
     * For mixed indexes, this returns the name of the configured indexing backend.
     *
     * @return
     */
    String getBackingIndex();

    /**
     * Returns which element type is being indexed by this index (vertex, edge, or property)
     *
     * @return
     */
    Class<? extends Element> getIndexedElement();

    /**
     * Returns the indexed keys of this index. If the returned array contains more than one element, its a
     * composite index.
     *
     * @return
     */
    PropertyKey[] getFieldKeys();

    /**
     * Returns the parameters associated with an indexed key of this index. Parameters modify the indexing
     * behavior of the underlying indexing backend.
     *
     * @param key
     * @return
     */
    Parameter[] getParametersFor(PropertyKey key);

    /**
     * Whether this is a unique index, i.e. values are uniquely associated with at most one element in the graph (for
     * a particular type)
     *
     * @return
     */
    boolean isUnique();

    /**
     * Returns the status of this index with respect to the provided PropertyKey.
     * For composite indexes, the key is ignored and the status of the index as a whole is returned.
     * For mixed indexes, the status of that particular key within the index is returned.
     *
     * @return
     */
    SchemaStatus getIndexStatus(PropertyKey key);

    /**
     * Whether this is a composite index
     * @return
     */
    boolean isCompositeIndex();

    /**
     * Whether this is a mixed index
     * @return
     */
    boolean isMixedIndex();


}
