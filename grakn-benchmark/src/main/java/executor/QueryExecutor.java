/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package executor;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Query;
import ai.grakn.remote.RemoteGrakn;
import ai.grakn.util.SimpleURI;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;
import io.prometheus.client.exporter.PushGateway;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.stream.Stream;


import static ai.grakn.graql.Graql.count;

/**
 *
 */
public class QueryExecutor {

    private final String uri;
    private final String keyspace;

    static final ArrayList<Query> queries = new ArrayList<>();

    static {
//        queries.add(Graql.match(Graql.var("x").isa("company")).limit(5).get());
        queries.add(Graql.match(Graql.var("x").isa("company")).limit(5).get("x"));
//        queries.add(Graql.match(Graql.var("x").isa("person")).limit(5).get());
        queries.add(Graql.match(Graql.var("x").isa("person")).limit(5).get("x"));
//        queries.add(Graql.match(Graql.var("x").isa("name")).limit(5).get());
        queries.add(Graql.match(Graql.var("x").isa("name")).limit(5).get("x"));
        queries.add(Graql.match(Graql.var("x").isa("company").has("name", "Google")).limit(5).get("x"));

        queries.add(Graql.match(Graql.var("x").isa("company").has("name", "Facebook").has("rating", 40)).limit(5).get("x"));
        queries.add(Graql.match(Graql.var("x").isa("employment")
                .rel("employer", Graql.var("c"))
                .rel("employee", Graql.var("e"))
                .has("name", "Facebook")
                .has("rating", 40))
                .limit(5).get("e"));
        queries.add(Graql.match(Graql.var("x").isa("company").has("name", "JetBrains")).aggregate(count()));
        queries.add(Graql.match(Graql.var("x").isa("rating")).aggregate(count()));
        queries.add(Graql.match(Graql.var("x").isa("name").has("rating", Graql.var("r"))).aggregate(count()));
        queries.add(Graql.match(Graql.var("x").isa("name").has("rating", 5)).aggregate(count()));
    }

    public QueryExecutor(String keyspace, String uri) {
        this.keyspace = keyspace;
        this.uri = uri;
    }

    public void processStaticQueries(int numRepeats, int numConcepts) {
        try {
            this.processQueries(queries.stream(), numRepeats, numConcepts);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void processQueries(Stream<Query> queryStream, int numRepeats, int numConcepts) throws Exception {

        Random rand = new Random();
        GraknSession session = RemoteGrakn.session(new SimpleURI(uri), Keyspace.of(keyspace));
        try (GraknTx tx = session.open(GraknTxType.WRITE)) {

            CollectorRegistry registry = new CollectorRegistry();
            Summary duration = Summary.build()
//                    .quantile(0.5, 0.05)
//                    .quantile(0.9, 0.01)
//                    .quantile(0.1, 0.01)
                    .name("grakn_queries_duration_seconds")  // TODO Why does it always fail to find data if this name is changed?!
                    .help("Duration of my batch job in seconds.")
                    .labelNames("method", "num_concepts")
                    .register(registry);

            Iterator<Query> queryIterator = queryStream.iterator();
            while (queryIterator.hasNext()){
                for (int i = 0; i <= rand.nextInt(numRepeats); i++) {

                    Query query = queryIterator.next().withTx(tx);
                    Summary.Timer durationTimer = duration.labels(query.toString(), String.valueOf(numConcepts)).startTimer();
                        try {
                            query.execute();
                        } finally {
                            durationTimer.observeDuration();
                        }
                }
            }

            // This is only added to the registry after success,
            // so that a previous success in the Pushgateway is not overwritten on failure.
            Gauge lastSuccess = Gauge.build()
                    .name("grakn_queries_duration_seconds_last_success_unixtime")
                    .help("Last time my batch job succeeded, in unixtime.")
                    .register(registry);
            lastSuccess.setToCurrentTime();
            PushGateway pg = new PushGateway("127.0.0.1:9091");
            pg.pushAdd(registry, "grakn_queries");
        }
    }

    void processQueriesBrave() {

//        reporter = AsyncReporter.create(URLConnectionSender.create("http://localhost:9411/api/v2/spans"));
//
//// Schedules the span to be sent, and won't block the calling thread on I/O
//        reporter.report(span);


        // Start a new trace or a span within an existing trace representing an operation
        Span span = tracer.nextSpan().name("encode").start();
// Put the span in "scope" so that downstream code such as loggers can see trace IDs
        try (SpanInScope ws = tracer.withSpanInScope(span)) {
            return encoder.encode();
        } catch (RuntimeException | Error e) {
            span.error(e); // Unless you handle exceptions, you might not know the operation failed!
            throw e;
        } finally {
            span.finish(); // note the scope is independent of the span. Always finish a span.
        }
    }

    public static void main(String[] args) {
        String uri = "localhost:48555";
        String keyspace = "societal_model";
        QueryExecutor queryExecutor = new QueryExecutor(keyspace, uri);
        try {
            queryExecutor.processStaticQueries(100, 400);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
