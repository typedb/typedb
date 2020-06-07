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

package grakn.core.graph.diskstorage.common;


import com.google.common.collect.Lists;
import grakn.core.graph.diskstorage.EntryMetaData;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreFeatures;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreManager;
import grakn.core.graph.diskstorage.util.StaticArrayEntry;

import java.util.List;

import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.STORE_META_TIMESTAMPS;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.STORE_META_TTL;

/**
 * Abstract Store Manager used as the basis for concrete StoreManager implementations.
 * Simplifies common configuration management.
 */

public abstract class AbstractStoreManager implements StoreManager {

    protected final Configuration storageConfig;

    public AbstractStoreManager(Configuration storageConfig) {
        this.storageConfig = storageConfig;
    }

    protected Configuration getStorageConfig() {
        return storageConfig;
    }

    public EntryMetaData[] getMetaDataSchema(String storeName) {
        List<EntryMetaData> schemaBuilder = Lists.newArrayList();
        StoreFeatures features = getFeatures();
        if (features.hasTimestamps() && storageConfig.get(STORE_META_TIMESTAMPS, storeName)) {
            schemaBuilder.add(EntryMetaData.TIMESTAMP);
        }
        if (features.hasCellTTL() && storageConfig.get(STORE_META_TTL, storeName)) {
            schemaBuilder.add(EntryMetaData.TTL);
        }
        if (schemaBuilder.isEmpty()) {
            return StaticArrayEntry.EMPTY_SCHEMA;
        }
        return schemaBuilder.toArray(new EntryMetaData[schemaBuilder.size()]);
    }

}
