/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.test.benchmark;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.EntityType;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Match;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.test.rule.SessionContext;
import org.junit.Rule;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

import java.util.Optional;

import static ai.grakn.graql.Graql.var;


public class MatchBenchmark extends BenchmarkTest {

    private static final String BENCHMARK_ENTITY_TYPE = "benchmarkEntityType";
    private static final String BENCHMARK_ATTRIBUTE_TYPE = "benchmarkAttributeType";

    @Rule
    public final SessionContext sessionContext = SessionContext.create();

    private GraknTx graph;

    @Setup
    public void setup() throws Throwable {
        GraknSession session = sessionContext.newSession();
        GraknTx graphEntity = session.transaction(GraknTxType.WRITE);
        EntityType entityType = graphEntity.putEntityType(BENCHMARK_ENTITY_TYPE);
        AttributeType<String> attributeType =
                graphEntity.putAttributeType(BENCHMARK_ATTRIBUTE_TYPE, AttributeType.DataType.STRING);
        entityType.has(attributeType);

        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 100; j++) {
                entityType.create().has(attributeType.create(String.valueOf(i)));
            }
        }
        graphEntity.commit();
        graph = session.transaction(GraknTxType.WRITE);
    }

    @TearDown
    public void tearDown() {
        graph.close();
    }

    @Benchmark
    public void match() {
        Match match = graph.graql().match(
                var("x")
                        .isa(BENCHMARK_ENTITY_TYPE)
                        .has(BENCHMARK_ATTRIBUTE_TYPE, "0")
        );
        GetQuery answers = match.get();
        Optional<ConceptMap> first = answers.stream().findFirst();
        first.get();
    }
}
