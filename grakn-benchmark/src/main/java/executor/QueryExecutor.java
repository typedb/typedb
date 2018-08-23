/*
 *  GRAKN.AI - THE KNOWLEDGE GRAPH
 *  Copyright (C) 2018 Grakn Labs Ltd
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package executor;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.client.Grakn;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Query;
import ai.grakn.util.SimpleURI;
import brave.Span;
import brave.Tracer;
import brave.Tracing;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 */
public class QueryExecutor {

    private final String uri;
    private final String dataSetName;
    private final String keyspace;

    final List<Query> queries;

//    static {
////        queries.add(Graql.match(Graql.var("x").isa("company")).limit(5).get());
//        queries.add(Graql.match(Graql.var("x").isa("company")).limit(5).get("x"));
////        queries.add(Graql.match(Graql.var("x").isa("person")).limit(5).get());
//        queries.add(Graql.match(Graql.var("x").isa("person")).limit(5).get("x"));
////        queries.add(Graql.match(Graql.var("x").isa("name")).limit(5).get());
//        queries.add(Graql.match(Graql.var("x").isa("name")).limit(5).get("x"));
//        queries.add(Graql.match(Graql.var("x").isa("company").has("name", "Google")).limit(5).get("x"));
//
//        queries.add(Graql.match(Graql.var("x").isa("company").has("name", "Facebook").has("rating", 40)).limit(5).get("x"));
//        queries.add(Graql.match(Graql.var("x").isa("employment")
//                .rel("employer", Graql.var("c"))
//                .rel("employee", Graql.var("e"))
//                .has("name", "Facebook")
//                .has("rating", 40))
//                .limit(5).get("e"));
//        queries.add(Graql.match(Graql.var("x").isa("company").has("name", "JetBrains")).aggregate(count()));
//        queries.add(Graql.match(Graql.var("x").isa("rating")).aggregate(count()));
//        queries.add(Graql.match(Graql.var("x").isa("name").has("rating", Graql.var("r"))).aggregate(count()));
//        queries.add(Graql.match(Graql.var("x").isa("name").has("rating", 5)).aggregate(count()));
//    }

    public QueryExecutor(String keyspace, String uri, String dataSetName, List<String> queryStrings) {
        this.keyspace = keyspace;
        this.uri = uri;
        this.dataSetName = dataSetName;

        // convert Graql strings into Query types
        this.queries = queryStrings.stream()
                        .map(q -> (Query)Graql.parser().parseQuery(q))
                        .collect(Collectors.toList());
    }

    public void processStaticQueries(int numRepeats, int numConcepts, Number runTimeStamp) {
        try {
            this.processQueries(queries.stream(), numRepeats, numConcepts, runTimeStamp);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void processQueries(Stream<Query> queryStream, int numRepeats, int numConcepts, Number runTimeStamp) throws Exception {
        // create tracing before the Grakn client is instantiated
//        AsyncReporter<zipkin2.Span> reporter = AsyncReporter.create(URLConnectionSender.create("http://localhost:9411/api/v2/spans"));
//        Tracing tracing = Tracing.newBuilder()
//                .localServiceName("query-benchmark-client-entry")
//                .spanReporter(reporter)
//                .supportsJoin(true)
//                .build();
//        Tracer tracer = tracing.tracer();
//        System.out.println("QueryExecutor tracer: ");
//        System.out.println(tracer);

        // instantiate grakn client
        Grakn client = new Grakn(new SimpleURI(uri));
        Grakn.Session session = client.session(Keyspace.of(keyspace));

        try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {


//             BraveTracer braveTracer = BraveTracer.create(tracing);
            Tracer tracer = Tracing.currentTracer();

            Iterator<Query> queryIterator = queryStream.iterator();

            int counter = 0;
            while (queryIterator.hasNext()) {

                Query query = queryIterator.next().withTx(tx);

                Span batchSpan = tracer.newTrace().name("batch-query-span");
                batchSpan.tag("numConcepts", Integer.toString(numConcepts));
                batchSpan.tag("query", query.toString());
                batchSpan.tag("dataSetName", this.dataSetName);
                batchSpan.tag("runStartDateTime", runTimeStamp.toString());
                batchSpan.start();


//                BraveSpanBuilder queryBatchSpanBuilder = braveTracer.buildSpan("querySpan")
//                        .withTag("numConcepts", numConcepts)
//                        .withTag("query", query.toString())
//                        .withTag("dataSetName", this.dataSetName)
//                        .withTag("runStartDateTime", runTimeStamp);

//                BraveSpan queryBatchSpan = queryBatchSpanBuilder.start();

                for (int i = 0; i < numRepeats; i++) {

//                    BraveSpanBuilder querySpanBuilder = braveTracer.buildSpan("querySpan")
//                            .withTag("repetition", i)
//                            .asChildOf(queryBatchSpan);

//                    ScopedSpan span = tracer.startScopedSpanWithParent("query-span", batchSpan.context());
                    Span span = tracer.newChild(batchSpan.context()).name("query-span");
                    span.tag("repetition", Integer.toString(i));
                    span.start();

                    try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
                        query.execute();
                    } catch (RuntimeException | Error e) {
                        span.error(e);
                        throw e;
                    } finally {
                        span.finish();
                    }
                    counter ++;
                }
//                Thread.sleep(2000);
                batchSpan.finish();
                Thread.sleep(500);
            }
            Thread.sleep(1500);
            System.out.println(counter);
        }
    }


}
