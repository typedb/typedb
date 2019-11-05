/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

import org.apache.tinkerpop.gremlin.structure.Direction;
import grakn.core.graph.diskstorage.EntryList;
import grakn.core.graph.diskstorage.util.CacheMetricsAction;
import grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration;
import grakn.core.graph.graphdb.database.cache.SchemaCache;
import grakn.core.graph.graphdb.database.cache.StandardSchemaCache;
import grakn.core.graph.graphdb.types.system.BaseRelationType;
import grakn.core.graph.util.stats.MetricManager;


public class MetricInstrumentedSchemaCache implements SchemaCache {

    public static final String METRICS_NAME = "schemacache";

    public static final String METRICS_TYPENAME = "name";
    public static final String METRICS_RELATIONS = "relations";

    private final SchemaCache cache;

    public MetricInstrumentedSchemaCache(StoreRetrieval retriever) {
        cache = new StandardSchemaCache(new StoreRetrieval() {
            @Override
            public Long retrieveSchemaByName(String typeName) {
                incAction(METRICS_TYPENAME, CacheMetricsAction.MISS);
                return retriever.retrieveSchemaByName(typeName);
            }

            @Override
            public EntryList retrieveSchemaRelations(long schemaId, BaseRelationType type, Direction dir) {
                incAction(METRICS_RELATIONS, CacheMetricsAction.MISS);
                return retriever.retrieveSchemaRelations(schemaId, type, dir);
            }
        });
    }

    private void incAction(String type, CacheMetricsAction action) {
        //todo-reenable
//        MetricManager.INSTANCE.getCounter(GraphDatabaseConfiguration.METRICS_SYSTEM_PREFIX_DEFAULT, METRICS_NAME, type, action.getName()).inc();
    }

    @Override
    public Long getSchemaId(String schemaName) {
        incAction(METRICS_TYPENAME, CacheMetricsAction.RETRIEVAL);
        return cache.getSchemaId(schemaName);
    }

    @Override
    public EntryList getSchemaRelations(long schemaId, BaseRelationType type, Direction dir) {
        incAction(METRICS_RELATIONS, CacheMetricsAction.RETRIEVAL);
        return cache.getSchemaRelations(schemaId, type, dir);
    }

    @Override
    public void expireSchemaElement(long schemaId) {
        cache.expireSchemaElement(schemaId);
    }

}
