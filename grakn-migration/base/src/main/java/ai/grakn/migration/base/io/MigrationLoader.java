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

import ai.grakn.engine.loader.Loader;
import ai.grakn.engine.loader.LoaderImpl;
import ai.grakn.migration.base.Migrator;
import ai.grakn.GraknGraph;

public class MigrationLoader {

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
        }
    }
}
