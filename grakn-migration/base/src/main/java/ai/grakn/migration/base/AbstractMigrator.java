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

package ai.grakn.migration.base;

import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.QueryBuilderImpl;
import ai.grakn.graql.macro.Macro;

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

    /**
     * @param template a string representing a templated graql query
     * @param data data used in the template
     * @return an insert query
     */
    protected InsertQuery template(String template, Map<String, Object> data){
        return (InsertQuery) queryBuilder.parseTemplate(template, data);
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
     * Test if an object is a valid Grakn value
     * @param value object to check
     * @return if the value is valid
     */
    protected boolean validValue(Object value){
        return value != null;
    }
}
