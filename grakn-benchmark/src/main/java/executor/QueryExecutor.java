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
import java.util.Date;
import java.util.Iterator;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.count;

/**
 *
 */
public class QueryExecutor {

    private final String uri;
    private final String dataSetName;
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

    public QueryExecutor(String keyspace, String uri, String dataSetName) {
        this.keyspace = keyspace;
        this.uri = uri;
        this.dataSetName = dataSetName;
    }

    public void processStaticQueries(int numRepeats, int numConcepts, Number runTimeStamp) {
        try {
            this.processQueries(queries.stream(), numRepeats, numConcepts, runTimeStamp);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void processQueries(Stream<Query> queryStream, int numRepeats, int numConcepts, Number runTimeStamp) throws Exception {
        Grakn client = new Grakn(new SimpleURI(uri));
        Grakn.Session session = client.session(Keyspace.of(keyspace));

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
                        .withTag("query", query.toString())
                        .withTag("dataSetName", this.dataSetName)
                        .withTag("runStartDateTime", runTimeStamp);

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
            System.out.println(counter);
            tracing.close();
        }
    }


    public static void main(String[] args) {
        String uri = "localhost:48555";
        String keyspace = "societal_model";
        QueryExecutor queryExecutor = new QueryExecutor(keyspace, uri, "generated_societal_model");
//        queryExecutor.processQueriesBrave();
        try {
            queryExecutor.processStaticQueries(100, 400, new Date().getTime());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
