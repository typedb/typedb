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

package io.mindmaps.migration.json;

import io.mindmaps.engine.loader.Loader;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Migrator for migrating JSON data into Mindmaps instances
 */
public class JsonMigrator {

    private static Logger LOG = LoggerFactory.getLogger(JsonMigrator.class);

    private static final int BATCH_SIZE = 5;

    private final Loader loader;
    private int batchSize = BATCH_SIZE;

    /**
     * Create a JsonMigrator to migrate into the given graph
     */
    public JsonMigrator(Loader loader) {
        this.loader = loader;
    }

    /**
     * Set number of files/objects to migrate in one batch
     * @param batchSize number of objects to migrate at once
     */
    public JsonMigrator setBatchSize(int batchSize){
        this.batchSize = batchSize;
        return this;
    }

    public void migrate(String template, File file){
        checkBatchSize();



    }

    public void graql(String template, File file){
        checkBatchSize();


    }


    /**
     * Warn the user when the batch size of the loader is greater than 1.
     * If the batch size is greater than 1, it is possible that multiple of the same variables will be committed in
     * one batch and the resulting committed data will be corrupted.
     */
    private void checkBatchSize(){
        if(loader.getBatchSize() > 1){
            LOG.warn("Loading with batch size [" + loader.getBatchSize() + "]. This can cause conflicts on commit.");
        }
    }
}
