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

import com.google.common.collect.Iterators;
import io.mindmaps.engine.loader.Loader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class AbstractTemplatedMigrator {

    public static final int BATCH_SIZE = 5;
    protected int batchSize = BATCH_SIZE;
    protected Loader loader;
    protected static Logger LOG = LoggerFactory.getLogger(AbstractTemplatedMigrator.class);

    /**
     * Migrate all the data in the given file based on the given template.
     * @param template parametrized graql insert query
     * @param file file containing data to be migrated
     */
    public abstract void migrate(String template, File file);

    /**
     * Migrate all the data in the given file based on the given template.
     * @param template parametrized graql insert query
     * @param file file containing data to be migrated
     * @return Graql insert statement representing the file
     */
    public abstract String graql(String template, File file);

    /**
     * Set number of rows to migrate in one batch
     * @param batchSize number of rows to migrate at once
     */
    public AbstractTemplatedMigrator setBatchSize(int batchSize){
        this.batchSize = batchSize;
        return this;
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

    /**
     * Partition a stream into a stream of collections, each with batchSize elements.
     * @param iterator Iterator to partition
     * @param <T> Type of values of iterator
     * @return Stream over a collection that are each of batchSize
     */
    protected <T> Stream<Collection<T>> partitionedStream(Iterator<T> iterator){
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                Iterators.partition(iterator, batchSize), Spliterator.ORDERED), false);
    }}
