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

import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.exception.GraqlTemplateParsingException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.internal.query.QueryBuilderImpl;
import ai.grakn.graql.macro.Macro;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class AbstractMigrator implements Migrator {

    private static final ConfigProperties properties = ConfigProperties.getInstance();

    private final static Logger LOG = LoggerFactory.getLogger(AbstractMigrator.class);
    private final QueryBuilderImpl queryBuilder = (QueryBuilderImpl) Graql.withoutGraph().infer(false);
    public static final int BATCH_SIZE = 25;

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
    protected Optional<InsertQuery> template(String template, Map<String, Object> data){
        try {
            return Optional.of(queryBuilder.parseTemplate(template, data));
        } catch (GraqlTemplateParsingException e){
            LOG.warn("Query was not sent to loader- " + e.getMessage());
            LOG.warn("See the Grakn engine logs for more detail about loading status and any resulting stacktraces: " + properties.getLogFilePath());
        }

        return Optional.empty();
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
