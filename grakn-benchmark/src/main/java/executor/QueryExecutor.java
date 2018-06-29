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
import ai.grakn.graql.admin.Answer;
import ai.grakn.remote.RemoteGrakn;
import ai.grakn.util.SimpleURI;
import com.codahale.metrics.MetricRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;
import io.prometheus.client.exporter.PushGateway;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.count;

/**
 *
 */
public class QueryExecutor {

    private static String uri = "localhost:48555";
    private static String keyspace = "societal_model";

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

    private long total;

    //    static final Summary requestLatency = Summary.build()
//            .name("requests_latency_seconds").help("Request latency in seconds.").register();
//
    public void processStaticQueries() {
        try {
            this.processQueries(queries.stream());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
//
//    //TODO name runExecute?
//    private Stream<Answer> processQueries(Stream<Query> queryStream) {
//
//        // Needs to take a stream of queries, execute them, and also pass the results back to the source of the stream
//
////        CollectorRegistry registry = new CollectorRegistry();
//
////        Summary requestLatency = Summary.build()
////                .name("requests_latency_seconds")
////                .help("Request latency in seconds.")
////                .register(registry);
//
//        GraknSession session = RemoteGrakn.session(new SimpleURI(uri), Keyspace.of(keyspace));
//
//        Iterator<Query> it = queryStream.iterator();
////        while (it.hasNext()) {
//            try (GraknTx tx = session.open(GraknTxType.WRITE)) {
//                Query query = it.next().withTx(tx);
//                System.out.println("Processing query " + query);
//
//                // Start the timer
//                Summary.Timer requestTimer = requestLatency.startTimer();
//                try {
//                    // Code to time
//                    query.execute();
//                } finally {
//                    // Stop the timer
//                    requestTimer.observeDuration();
//                }
//            }
////        }
//        return null;
//    }

    //TODO name runExecute?
    private Stream<Answer> processQueries(Stream<Query> queryStream) throws Exception {

        // Needs to take a stream of queries, execute them, and also pass the results back to the source of the stream

        CollectorRegistry registry = new CollectorRegistry();

        Summary requestLatency = Summary.build()
                .name("request_latency_seconds")
                .help("Request latency in seconds.")
                .register(registry);


        GraknSession session = RemoteGrakn.session(new SimpleURI(uri), Keyspace.of(keyspace));

        Iterator<Query> it = queryStream.iterator();
//        while (it.hasNext()) {
        try (GraknTx tx = session.open(GraknTxType.WRITE)) {
            Query query = it.next().withTx(tx);
            System.out.println("Processing query " + query);

            // Start the timer
            Summary.Timer requestTimer = requestLatency.startTimer();
            try {
                // Code to time
                query.execute();

                Gauge lastSuccess = Gauge.build()
                        .name("request_latency_seconds_batch_unixtime")
                        .help("Last time my batch job succeeded, in unixtime.")
                        .register(registry);
                lastSuccess.setToCurrentTime();

            } finally {
                // Stop the timer
                requestTimer.observeDuration();
                // Push the batch of results to the server
                PushGateway pg = new PushGateway("127.0.0.1:9091");
                pg.pushAdd(registry, "grakn_queries");
            }
        }
//        }
        return null;
    }

//    void executeBatchJob() throws Exception {
//
//        Random rand = new Random();
//        // Your code here.
//        GraknSession session = RemoteGrakn.session(new SimpleURI(uri), Keyspace.of(keyspace));
//        try (GraknTx tx = session.open(GraknTxType.WRITE)) {
//
//            CollectorRegistry registry = new CollectorRegistry();
//            Summary duration = Summary.build()
//                    .quantile(0.5, 0.05)
//                    .quantile(0.9, 0.01)
//                    .quantile(0.1, 0.01)
//                    .name("grakn_queries_duration_seconds")  // TODO Why does it always fail to find data if this name is changed?!
//                    .help("Duration of my batch job in seconds.")
//                    .labelNames("method")
//                    .register(registry);
//            this.total = 0;
//            for (int i = 0; i <= rand.nextInt(100); i++) {
//
//                long j = rand.nextInt(10);
//                duration.labels(String.valueOf(j)).time(() -> {
//                    // Your code here.
////                    Query query = queries.get(4).withTx(tx);
////                    query.execute();
//                    try {
//                        this.total += j;
//                        TimeUnit.MILLISECONDS.sleep(j);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                });
//            }
//            System.out.println("Total: " + total);
//
//
////            Summary.Timer durationTimer = duration.startTimer();
////            try {
////                Query query = queries.get(4).withTx(tx);
////                query.execute();
////
////            } finally {
////                durationTimer.observeDuration();
////            }
////            }
//            // This is only added to the registry after success,
//            // so that a previous success in the Pushgateway is not overwritten on failure.
//            Gauge lastSuccess = Gauge.build()
//                    .name("grakn_queries_duration_seconds_last_success_unixtime")
//                    .help("Last time my batch job succeeded, in unixtime.")
//                    .register(registry);
//            lastSuccess.setToCurrentTime();
//            PushGateway pg = new PushGateway("127.0.0.1:9091");
//            pg.pushAdd(registry, "grakn_queries");
//        }
//    }


    void executeBatchJob() throws Exception {

        Random rand = new Random();
        // Your code here.
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

            for (Query query : queries) {
                for (int i = 0; i <= rand.nextInt(100); i++) {

                    query = query.withTx(tx);
                    Summary.Timer durationTimer = duration.labels(query.toString(), "400").startTimer();
                        try {
                            query.execute();
                        } finally {
                            durationTimer.observeDuration();
                        }
                }
            }
            
            for (Query query : queries) {
                for (int i = 0; i <= rand.nextInt(100); i++) {

                    query = query.withTx(tx);
                    Summary.Timer durationTimer = duration.labels(query.toString(), "1000").startTimer();
//                    duration.labels(query.toString()).time(() -> {
                        // Your code here.
                        try {
                            TimeUnit.MILLISECONDS.sleep(50);
                            query.execute();
                        } finally {
                            durationTimer.observeDuration();
                        }

//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }
//                    });
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

    public static void main(String[] args) {
        QueryExecutor queryExecutor = new QueryExecutor();
//        queryExecutor.processStaticQueries();
        try {
            queryExecutor.executeBatchJob();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
