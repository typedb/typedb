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

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import grakn.core.graph.diskstorage.EntryList;
import grakn.core.graph.graphdb.idmanagement.IDManager;
import grakn.core.graph.graphdb.relations.EdgeDirection;
import grakn.core.graph.graphdb.types.system.BaseKey;
import grakn.core.graph.graphdb.types.system.BaseLabel;
import grakn.core.graph.graphdb.types.system.BaseRelationType;
import grakn.core.graph.graphdb.types.system.SystemRelationType;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class StandardSchemaCache implements SchemaCache {

    private static final int MAX_CACHED_TYPES_DEFAULT = 10000;
    private static final int INITIAL_CAPACITY = 128;
    private static final int INITIAL_CACHE_SIZE = 16;
    private static final int CACHE_RELATION_MULTIPLIER = 3; // 1) type-name, 2) type-definitions, 3) modifying edges [index, lock]
    private static final int CONCURRENCY_LEVEL = 2;

    private static final int SCHEMAID_TOTALFORW_SHIFT = 3; //Total number of bits appended - the 1 is for the 1 bit direction
    private static final int SCHEMAID_BACK_SHIFT = 2; //Number of bits to remove from end of schema id since its just the padding

    //The following two conditions should always be true if we ever decide to change any of the above fields
//        IDManager.VertexIDType.Schema.removePadding(1L << SCHEMAID_BACK_SHIFT) == 1;
//        SCHEMAID_TOTALFORW_SHIFT - SCHEMAID_BACK_SHIFT >= 0;

    private final int maxCachedTypes;
    private final int maxCachedRelations;
    private final StoreRetrieval retriever;

    private volatile ConcurrentMap<String, Long> typeNames;
    private final Cache<String, Long> typeNamesBackup;

    private volatile ConcurrentMap<Long, EntryList> schemaRelations;
    private final Cache<Long, EntryList> schemaRelationsBackup;

    public StandardSchemaCache(StoreRetrieval retriever) {
        this(MAX_CACHED_TYPES_DEFAULT, retriever);
    }

    private StandardSchemaCache(int size, StoreRetrieval retriever) {
        Preconditions.checkArgument(size > 0, "Size must be positive");
        Preconditions.checkNotNull(retriever);
        maxCachedTypes = size;
        maxCachedRelations = maxCachedTypes * CACHE_RELATION_MULTIPLIER;
        this.retriever = retriever;

        typeNamesBackup = CacheBuilder.newBuilder()
                .concurrencyLevel(CONCURRENCY_LEVEL).initialCapacity(INITIAL_CACHE_SIZE)
                .maximumSize(maxCachedTypes).build();
        typeNames = new ConcurrentHashMap<>(INITIAL_CAPACITY, 0.75f, CONCURRENCY_LEVEL);

        schemaRelationsBackup = CacheBuilder.newBuilder()
                .concurrencyLevel(CONCURRENCY_LEVEL).initialCapacity(INITIAL_CACHE_SIZE * CACHE_RELATION_MULTIPLIER)
                .maximumSize(maxCachedRelations).build();
        schemaRelations = new ConcurrentHashMap<>(INITIAL_CAPACITY * CACHE_RELATION_MULTIPLIER, 0.75f, CONCURRENCY_LEVEL);
    }


    @Override
    public Long getSchemaId(String schemaName) {
        ConcurrentMap<String, Long> types = typeNames;
        Long id;
        if (types == null) {
            id = typeNamesBackup.getIfPresent(schemaName);
            if (id == null) {
                id = retriever.retrieveSchemaByName(schemaName);
                if (id != null) { //only cache if type exists
                    typeNamesBackup.put(schemaName, id);
                }
            }
        } else {
            id = types.get(schemaName);
            if (id == null) { //Retrieve it
                if (types.size() > maxCachedTypes) {
                    /* Safe guard against the concurrent hash map growing to large - this would be a VERY rare event
                    as it only happens for graph databases with thousands of types.
                     */
                    typeNames = null;
                    return getSchemaId(schemaName);
                } else {
                    //Expand map
                    id = retriever.retrieveSchemaByName(schemaName);
                    if (id != null) { //only cache if type exists
                        types.put(schemaName, id);
                    }
                }
            }
        }
        return id;
    }

    private long getIdentifier(long schemaId, SystemRelationType type, Direction dir) {
        int edgeDir = EdgeDirection.position(dir);

        long typeId = (schemaId >>> SCHEMAID_BACK_SHIFT);
        int systemTypeId;
        if (type == BaseLabel.SchemaDefinitionEdge) {
            systemTypeId = 0;
        } else if (type == BaseKey.SchemaName) {
            systemTypeId = 1;
        } else if (type == BaseKey.SchemaCategory) {
            systemTypeId = 2;
        } else if (type == BaseKey.SchemaDefinitionProperty) {
            systemTypeId = 3;
        } else {
            throw new AssertionError("Unexpected SystemType encountered in StandardSchemaCache: " + type.name());
        }

        return (((typeId << 2) + systemTypeId) << 1) + edgeDir;
    }

    @Override
    public EntryList getSchemaRelations(long schemaId, BaseRelationType type, Direction dir) {
        Preconditions.checkArgument(IDManager.VertexIDType.Schema.is(schemaId));
        Preconditions.checkArgument((Long.MAX_VALUE >>> (SCHEMAID_TOTALFORW_SHIFT - SCHEMAID_BACK_SHIFT)) >= schemaId);

        long typePlusRelation = getIdentifier(schemaId, type, dir);
        ConcurrentMap<Long, EntryList> types = schemaRelations;
        EntryList entries;
        if (types == null) {
            entries = schemaRelationsBackup.getIfPresent(typePlusRelation);
            if (entries == null) {
                entries = retriever.retrieveSchemaRelations(schemaId, type, dir);
                if (!entries.isEmpty()) { //only cache if type exists
                    schemaRelationsBackup.put(typePlusRelation, entries);
                }
            }
        } else {
            entries = types.get(typePlusRelation);
            if (entries == null) { //Retrieve it
                if (types.size() > maxCachedRelations) {
                    /* Safe guard against the concurrent hash map growing to large - this would be a VERY rare event
                    as it only happens for graph databases with thousands of types.
                     */
                    schemaRelations = null;
                    return getSchemaRelations(schemaId, type, dir);
                } else {
                    //Expand map
                    entries = retriever.retrieveSchemaRelations(schemaId, type, dir);
                    types.put(typePlusRelation, entries);
                }
            }
        }
        return entries;
    }

}
