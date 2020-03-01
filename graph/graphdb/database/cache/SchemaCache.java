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

package grakn.core.graph.graphdb.database.cache;

import grakn.core.graph.diskstorage.EntryList;
import grakn.core.graph.graphdb.types.system.BaseRelationType;
import org.apache.tinkerpop.gremlin.structure.Direction;

/**
 * This interface defines the methods that a SchemaCache must implement. A SchemaCache is maintained by the JanusGraph graph
 * database in order to make the frequent lookups of schema vertices and their attributes more efficient through a dedicated
 * caching layer. Schema vertices are type vertices and related vertices.
 * <p>
 * The SchemaCache speeds up two types of lookups:
 * <ul>
 *     <li>Retrieving a type by its name (index lookup)</li>
 *     <li>Retrieving the relations of a schema vertex for predefined SystemRelationTypes</li>
 * </ul>
 */
public interface SchemaCache {

    Long getSchemaId(String schemaName);

    EntryList getSchemaRelations(long schemaId, BaseRelationType type, Direction dir);

    interface StoreRetrieval {

        Long retrieveSchemaByName(String typeName);

        EntryList retrieveSchemaRelations(long schemaId, BaseRelationType type, Direction dir);
    }

}
