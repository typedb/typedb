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

package grakn.core.graph.graphdb.types;

import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.schema.JanusGraphSchemaType;
import grakn.core.graph.graphdb.internal.ElementCategory;


public interface IndexType {

    ElementCategory getElement();

    IndexField[] getFieldKeys();

    IndexField getField(PropertyKey key);

    boolean indexesKey(PropertyKey key);

    boolean isCompositeIndex();

    boolean isMixedIndex();

    boolean hasSchemaTypeConstraint();

    JanusGraphSchemaType getSchemaTypeConstraint();

    String getBackingIndexName();

    String getName();

    /**
     * Resets the internal caches used to speed up lookups on this index.
     * This is needed when the index gets modified in ManagementSystem.
     */
    void resetCache();

    //TODO: Add in the future
    //public And getCondition();
}
