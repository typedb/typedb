/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.migration.base.io;

import ai.grakn.engine.loader.LoaderClient;
import ai.grakn.migration.base.AbstractMigrator;
import ai.grakn.migration.base.Migrator;

/**
 * Iterate over a migrator adding each result into the loader
 * @author alexandraorth
 */
public class MigrationLoader {

    public static void load(String uri, String keyspace, int batchSize, Migrator migrator){
        LoaderClient loader = new LoaderClient(keyspace, uri);
        loader.setBatchSize(batchSize);

        try{
            migrator.migrate().forEach(loader::add);
        } finally {
            loader.waitToFinish();
//            loader.printLoaderState();
        }
    }

    public static void load(String uri, String keyspace, Migrator migrator) {
        load(uri, keyspace, AbstractMigrator.BATCH_SIZE, migrator);
    }
}