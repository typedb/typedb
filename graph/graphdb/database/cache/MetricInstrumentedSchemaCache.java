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
