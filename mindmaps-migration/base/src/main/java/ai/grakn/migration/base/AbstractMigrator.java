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

package ai.grakn.migration.base;

import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.QueryBuilderImpl;
import ai.grakn.graql.internal.template.macro.Macro;

import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class AbstractMigrator implements Migrator {

    public static final int BATCH_SIZE = 25;
    public final QueryBuilderImpl queryBuilder = (QueryBuilderImpl) Graql.withoutGraph();

    /**
     * Register a macro to use in templating
     */
    public AbstractMigrator registerMacro(Macro macro){
        queryBuilder.registerMacro(macro);
        return this;
    }

    protected InsertQuery template(String template, Map<String, Object> data){
        String templated = queryBuilder.parseTemplate(template, data);
        return queryBuilder.parse(templated);
    }

    /**
     * Partition a stream into a stream of collections, each with batchSize elements.
     * @param iterator Iterator to partition
     * @param <T> Type of values of iterator
     * @return Stream over a collection that are each of batchSize
     */
    protected <T> Stream<T> stream(Iterator<T> iterator){
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);

    }

    /**
     * Test if an object is a valid Mindmaps value
     * @param value object to check
     * @return if the value is valid
     */
    protected boolean validValue(Object value){
        return value != null;
    }
}
