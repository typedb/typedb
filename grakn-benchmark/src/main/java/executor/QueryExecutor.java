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

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.client.Grakn;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Query;
import ai.grakn.util.SimpleURI;
import brave.Tracing;
import brave.opentracing.BraveSpan;
import brave.opentracing.BraveSpanBuilder;
import brave.opentracing.BraveTracer;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.urlconnection.URLConnectionSender;

import java.util.ArrayList;
import java.util.Iterator;
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

        Grakn.Session session = Grakn.session(new SimpleURI(uri), Keyspace.of(keyspace));
        try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {

            AsyncReporter<zipkin2.Span> reporter = AsyncReporter.create(URLConnectionSender.create("http://localhost:9411/api/v2/spans"));

            Tracing tracing = Tracing.newBuilder()
                    .localServiceName("query-benchmark")
                    .spanReporter(reporter)
                    .build();

            BraveTracer braveTracer = BraveTracer.create(tracing);

            Iterator<Query> queryIterator = queryStream.iterator();

            int counter = 0;
            while (queryIterator.hasNext()) {

                Query query = queryIterator.next().withTx(tx);

                BraveSpanBuilder queryBatchSpanBuilder = braveTracer.buildSpan("querySpan")
                        .withTag("numConcepts", numConcepts)
                        .withTag("query", query.toString());

                BraveSpan queryBatchSpan = queryBatchSpanBuilder.start();

                for (int i = 0; i < numRepeats; i++) {

                    BraveSpanBuilder querySpanBuilder = braveTracer.buildSpan("querySpan")
                            .withTag("repetition", i)
                            .asChildOf(queryBatchSpan);

                    BraveSpan querySpan = querySpanBuilder.start();
                    query.execute();
                    querySpan.finish();
                    counter ++;
                }
                queryBatchSpan.finish();
            }

            BraveSpanBuilder initialiserSpanBuilder = braveTracer.buildSpan("initialiser");
            BraveSpan initialiserSpan = initialiserSpanBuilder.start();
            Thread.sleep(2000);
            initialiserSpan.finish();

            System.out.println(counter);
            tracing.close();
        }
    }

//    void processQueriesBrave() {
//
////        // Configure a reporter, which controls how often spans are sent
////        //   (the dependency is io.zipkin.reporter2:zipkin-sender-okhttp3)
////        sender = OkHttpSender.create("http://127.0.0.1:9411/api/v2/spans");
////        AsyncReporter<zipkin2.Span> spanReporter = AsyncReporter.create(sender);
////
////        // Create a tracing component with the service name you want to see in Zipkin.
////        Tracing tracing = Tracing.newBuilder()
////                .localServiceName("my-service")
////                .spanReporter(spanReporter)
////                .build();
////
////        // Tracing exposes objects you might need, most importantly the tracer
////        Tracer tracer = tracing.tracer();
////
////
////        AsyncReporter<zipkin2.Span> reporter = AsyncReporter.create(URLConnectionSender.create("http://localhost:9411/api/v2/spans"));
////
////        // Schedules the span to be sent, and won't block the calling thread on I/O
////        reporter.report(span);
////
////
////        // Start a new trace or a span within an existing trace representing an operation
////        Span span = tracer.nextSpan().name("encode").start();
////// Put the span in "scope" so that downstream code such as loggers can see trace IDs
////        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
////            return encoder.encode();
////        } catch (RuntimeException | Error e) {
////            span.error(e); // Unless you handle exceptions, you might not know the operation failed!
////            throw e;
////        } finally {
////            span.finish(); // note the scope is independent of the span. Always finish a span.
////        }
//
//        // Configure a reporter, which controls how often spans are sent
//        //   (the dependency is io.zipkin.reporter2:zipkin-sender-okhttp3)
////        sender = OkHttpSender.create("http://127.0.0.1:9411/api/v2/spans");
////        AsyncReporter<zipkin2.Span> spanReporter = AsyncReporter.create(sender);
//
//
////        Tracing brave = Tracing.newBuilder().spanReporter(spans::add).build();
////        BraveTracer opentracing = BraveTracer.wrap(brave); //TODO Can't find wrap
//
//
//        AsyncReporter<zipkin2.Span> reporter = AsyncReporter.create(URLConnectionSender.create("http://localhost:9411/api/v2/spans"));
//
//        Tracing tracing = Tracing.newBuilder()
//                .localServiceName("my-service")
////        .spanReporter(spanReporter)
//                .spanReporter(reporter)
//                .build();
//
//        Tracer tracer = tracing.tracer();
//
////        Tracer tracer = Tracing.currentTracer();
////        Tracer tracer = tracing.tracer();
////        Span span = tracer.newTrace().name("encode").start();
////        try {
////            doSomethingExpensive();
////        } finally {
////            span.finish();
////        }
//
//        //TODO this looks good
//        BraveTracer braveTracer = BraveTracer.create(tracing);
//        BraveSpanBuilder serverCall = braveTracer.buildSpan("ServerCall");
////        serverCall.
//
//        Span twoPhase = tracer.newTrace().name("twoPhase").start();
////        try {
////        Span prepare = tracer.newChild(twoPhase.context()).name("prepare").start();
////            try {
////                prepare();
////        try {
////            Thread.sleep(500);
////        } catch (InterruptedException e) {
////            e.printStackTrace();
////        }
////        System.out.println("-- Phase 1: prepare --");
//////            } catch (InterruptedException e) {
//////                e.printStackTrace();
//////            } finally {
////        prepare.finish();
////            }
//
//        Span commit = tracer.newChild(twoPhase.context()).name("phase2").start();
////            try {
////                commit();
//        System.out.println("-- Phase 2: commit --");
//        try {
//            Thread.sleep(700);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
////            } catch (InterruptedException e) {
////                e.printStackTrace();
////            } finally {
//        commit.finish();
////            }
////        prepare.finish();
//
////        } catch (InterruptedException e) {
////            e.printStackTrace();
////        } finally {
//        twoPhase.finish();
////        }
//    }

    public static void main(String[] args) {
        String uri = "localhost:48555";
        String keyspace = "societal_model";
        QueryExecutor queryExecutor = new QueryExecutor(keyspace, uri);
//        queryExecutor.processQueriesBrave();
        try {
            queryExecutor.processStaticQueries(100, 400);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
