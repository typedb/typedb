/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.migration.base;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.engine.loader.BlockingLoader;
import io.mindmaps.engine.loader.Loader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Reader;

public class LoadingMigrator {

    protected static Logger LOG = LoggerFactory.getLogger(LoadingMigrator.class);
    private final Migrator migrator;
    private final Loader loader;

    public LoadingMigrator(MindmapsGraph graph, Migrator migrator){
        this(new BlockingLoader(graph.getKeyspace()), migrator);
    }

    public LoadingMigrator(Loader loader, Migrator migrator){
        this.loader = loader;
        this.migrator = migrator;
        loader.setBatchSize(1);
    }

    public void migrate(String template, File file) {
        checkBatchSize();
        try{
            migrator.migrate(template, file).forEach(loader::add);
        } finally {
            loader.waitToFinish();
        }
    }

    public void migrate(String template, Reader reader) {
        checkBatchSize();
        try{
            migrator.migrate(template, reader).forEach(loader::add);
        } finally {
            loader.waitToFinish();
        }
    }

    /**
     * Warn the user when the batch size of the loader is greater than 1.
     * If the batch size is greater than 1, it is possible that multiple of the same variables will be committed in
     * one batch and the resulting committed data will be corrupted.
     */
    protected void checkBatchSize(){
        if(loader.getBatchSize() > 1){
            LOG.warn("Loading with batch size [" + loader.getBatchSize() + "]. This can cause conflicts on commit.");
        }
    }
}
