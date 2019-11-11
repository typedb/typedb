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

package grakn.core.graph.diskstorage.common;

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.diskstorage.util.DirectoryUtil;

import java.io.File;

import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_ROOT;

/**
 * Abstract Store Manager used as the basis for local StoreManager implementations.
 * Simplifies common configuration management.
 */

public abstract class LocalStoreManager extends AbstractStoreManager {

    protected final File directory;

    public LocalStoreManager(Configuration storageConfig) throws BackendException {
        super(storageConfig);
        Preconditions.checkArgument(storageConfig.has(STORAGE_DIRECTORY) ||
                        (storageConfig.has(STORAGE_ROOT) && storageConfig.has(GRAPH_NAME)),
                String.format("Please supply configuration parameter \"%s\" or both \"%s\" and \"%s\".",
                        STORAGE_DIRECTORY.toStringWithoutRoot(),
                        STORAGE_ROOT.toStringWithoutRoot(),
                        GRAPH_NAME.toStringWithoutRoot()
                ));
        if (storageConfig.has(STORAGE_DIRECTORY)) {
            final String storageDir = storageConfig.get(STORAGE_DIRECTORY);
            directory = DirectoryUtil.getOrCreateDataDirectory(storageDir);
        } else {
            final String storageRoot = storageConfig.get(STORAGE_ROOT);
            final String graphName = storageConfig.get(GRAPH_NAME);
            directory = DirectoryUtil.getOrCreateDataDirectory(storageRoot, graphName);
        }
    }
}
