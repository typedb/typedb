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
import ai.grakn.graql.answer.Value;
import ai.grakn.util.SimpleURI;
import brave.Span;
import brave.Tracer;
import brave.Tracing;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.var;

/**
 *
 */
public class QueryExecutor {

    private final String uri;
    private final String executionName;
    private final String keyspace;

    final List<Query> queries;

    public QueryExecutor(String keyspace, String uri, String executionName, List<String> queryStrings) {
        this.keyspace = keyspace;
        this.uri = uri;
        this.executionName = executionName;

        // convert Graql strings into Query types
        this.queries = queryStrings.stream()
                        .map(q -> (Query)Graql.parser().parseQuery(q))
                        .collect(Collectors.toList());
    }

    public void processStaticQueries(int numRepeats, int numConcepts, String extraMessage) {
        try {
            this.processQueries(queries.stream(), numRepeats, numConcepts, extraMessage);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void processStaticQueries(int numRepeats, int numConcepts) {
        processStaticQueries(numRepeats, numConcepts, null);
    }

    public int aggregateCount() {
        Grakn client = new Grakn(new SimpleURI(uri));
        Grakn.Session session = client.session(Keyspace.of(keyspace));

        try (Grakn.Transaction tx = session.transaction(GraknTxType.READ)) {
            List<Value> count = tx.graql().match(var("x").isa("thing")).aggregate(Graql.count()).execute();
            return count.get(0).number().intValue();
        }
    }

    void processQueries(Stream<Query> queryStream, int numRepeats, int numConcepts, String msg) throws Exception {
        // instantiate grakn client
        Grakn client = new Grakn(new SimpleURI(uri));
        Grakn.Session session = client.session(Keyspace.of(keyspace));

        try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {


            Tracer tracer = Tracing.currentTracer();

            Iterator<Query> queryIterator = queryStream.iterator();
            int counter = 0;
            while (queryIterator.hasNext()) {

                Query query = queryIterator.next().withTx(tx);

                Span batchSpan = tracer.newTrace().name("batch-query-span");
                batchSpan.tag("concepts", Integer.toString(numConcepts));
                batchSpan.tag("query", query.toString());
                batchSpan.tag("executionName", this.executionName);
                if (msg != null) {
                    batchSpan.tag("extraTag", msg);
                }
                batchSpan.start();


                for (int i = 0; i < numRepeats; i++) {

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
                batchSpan.finish();
                Thread.sleep(500);
            }
            Thread.sleep(1500);
            System.out.println(counter);
        }
    }


}
