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

import ai.grakn.Grakn;
import ai.grakn.engine.loader.Loader;
import ai.grakn.engine.loader.LoaderImpl;
import ai.grakn.engine.loader.client.LoaderClient;
import ai.grakn.migration.base.Migrator;
import ai.grakn.GraknGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * Iterate over a migrator adding each result into the loader
 * @author alexandraorth
 */
public class MigrationLoader {

    private static final Logger LOG = LoggerFactory.getLogger(MigrationLoader.class);

    public static Loader getLoader(MigrationOptions options){
        return options.getUri().equals(Grakn.DEFAULT_URI)
                ? new LoaderImpl(options.getKeyspace())
                : new LoaderClient(options.getKeyspace(), Collections.singleton(options.getUri()));
    }

    public static void load(GraknGraph graph, Migrator migrator){
        load(new LoaderImpl(graph.getKeyspace()), migrator);
    }

    public static void load(Loader loader, int batchSize, Migrator migrator){
        loader.setBatchSize(batchSize);
        load(loader, migrator);
    }

    public static void load(Loader loader, Migrator migrator) {
        try{
            migrator.migrate().forEach(loader::add);
        } finally {
            loader.waitToFinish();
            LOG.info("Loading finished with status: " + loader.getLoaderState());
        }
    }
}