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

import ai.grakn.Keyspace;
import ai.grakn.client.BatchExecutorClient;
import ai.grakn.client.GraknClient;
import ai.grakn.client.QueryResponse;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryParser;
import ai.grakn.graql.macro.Macro;
import static ai.grakn.util.ConcurrencyUtil.allObservable;
import ai.grakn.util.SimpleURI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

/**
 * <p>
 * Abstract migrator class containing methods and functionality needed by
 * all extending migrator classes.
 * </p>
 *
 * @author alexandraorth
 */
public class Migrator {

    private final static AtomicInteger numberQueriesSubmitted = new AtomicInteger(0);

    private final static Logger LOG = LoggerFactory.getLogger(Migrator.class);
    private final QueryParser queryParser = Graql.withoutGraph().infer(false).parser();
    static final int DEFAULT_MAX_RETRY = 1;

    private final String uri;
    private final Keyspace keyspace;

    /**
     * @param uri Uri where one instance of Grakn Engine is running
     * @param keyspace The {@link Keyspace} where the data should be persisted
     */
    private Migrator(String uri, Keyspace keyspace) {
        this.uri = uri;
        this.keyspace = keyspace;
    }

    public static Migrator to(String uri, Keyspace keyspace) {
        return new Migrator(uri, keyspace);
    }

    /**
     * Register a macro to use in templating
     */
    public Migrator registerMacro(Macro macro) {
        queryParser.registerMacro(macro);
        return this;
    }

    /**
     * Migrate data constrained by this migrator using a loader configured
     * by the provided parameters.
     *
     * Uses the default batch size and number of active tasks.
     *
     * NOTE: Currently only used for testing purposes
     */
    public void load(String template, Stream<Map<String, Object>> converter) {
        load(template, converter, Migrator.DEFAULT_MAX_RETRY, true);
    }

    /**
     * Template the data and print to standard out.
     */
    public void print(String template, Stream<Map<String, Object>> converter) {
        converter.flatMap(d -> template(template, d, false))
                .forEach(System.out::println);
    }

    /**
     * Migrate data constrained by this migrator using a loader configured
     * by the provided parameters.
     *
     * @param retrySize If the Loader should continue attempt to send tasks when Engine is not
     * available or an exception occurs
     */
    public void load(String template, Stream<Map<String, Object>> converter, int retrySize, boolean failFast) {
        try (BatchExecutorClient loader =
                BatchExecutorClient.newBuilder()
                        .taskClient(new GraknClient(new SimpleURI(uri)))
                        .maxRetries(retrySize)
                        .build()) {
            if (!loader.keyspaceExists(keyspace)) {
                System.out.println("No such keyspace " + keyspace);
                return;
            }
            List<Observable<Optional<QueryResponse>>> all = new ArrayList<>();
            converter
                    .flatMap(d -> template(template, d, failFast))
                    .forEach(q -> {
                        LOG.debug("Adding query {}", q);
                        numberQueriesSubmitted.incrementAndGet();
                        Observable<Optional<QueryResponse>> addObservable = loader
                                .add(q, keyspace.getValue()).map(Optional::of);
                        if (!failFast) {
                            addObservable = addObservable.onErrorResumeNext(error -> {
                                LOG.error("Found error, skipping", error);
                                return Observable.just(Optional.empty());
                            });
                        }
                        all.add(addObservable);
                        addObservable.filter(Optional::isPresent).subscribe(
                                        taskResult -> LOG.debug("Successfully executed: {}", taskResult),
                                        error -> {
                                                if (failFast) {
                                                    throw GraknBackendException.migrationFailure(error.getMessage());
                                                }
                                        }
                                );
                    });
            int completed = allObservable(all).toBlocking().first().size();
            LOG.info("Loaded {} statements", completed);
        }
    }

    /**
     * @param template a string representing a templated graql query
     * @param data data used in the template
     * @param debug
     * @return an insert query
     */
    protected Stream<Query> template(String template, Map<String, Object> data, boolean debug) {
        try {
            return queryParser.parseTemplate(template, data);
        } catch (Exception e) {
            System.out.println("Query not sent to server: " + e.getMessage());
            if (debug) {
                throw e;
            }
        }

        return Stream.empty();
    }
}
