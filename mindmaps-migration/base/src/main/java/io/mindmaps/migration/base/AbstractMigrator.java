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
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.Pattern;
import io.mindmaps.graql.Var;
import io.mindmaps.graql.admin.PatternAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toSet;

public abstract class AbstractMigrator implements Migrator {

    public static final int BATCH_SIZE = 5;
    protected int batchSize = BATCH_SIZE;

    /**
     * Set number of rows to migrate in one batch
     * @param batchSize number of rows to migrate at once
     */
    public AbstractMigrator setBatchSize(int batchSize){
        this.batchSize = batchSize;
        return this;
    }

    public LoadingMigrator getLoadingMigrator(Loader loader){
        return new LoadingMigrator(loader, this);
    }

    protected Collection<Var> template(String template, Collection<Map<String, Object>> data){
        Map<String, Object> forData = Collections.singletonMap("data", data);
        String expandedQuery = Graql.parseTemplate(loopForBatch(template), forData);

        return Graql.parsePatterns(expandedQuery).stream()
                .map(Pattern::admin)
                .filter(PatternAdmin::isVar)
                .map(PatternAdmin::asVar)
                .collect(toSet());
    }

    protected String loopForBatch(String template){
        return "for(data) do { " + template + "}";
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
    }
}
