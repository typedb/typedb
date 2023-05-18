/*
 * Copyright (C) 2022 Vaticle
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
 *
 */

package com.vaticle.typedb.core.reasoner.benchmark.iam;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class ConjunctionStructureTest {

    private static final String database = "iam-benchmark-conjunctions";
    private static final BenchmarkRunner benchmarker = new BenchmarkRunner(database);
    private final QueryParams queryParams;

    public ConjunctionStructureTest() {
        queryParams = QueryParams.load();
    }

    @BeforeClass
    public static void setup() throws IOException {
        benchmarker.setUp();
        benchmarker.loadSchema("schema_types.tql");
        benchmarker.loadSchema("schema_rules_optimised.tql");
        benchmarker.importData("data.typedb");
        benchmarker.warmUp();
    }

    @AfterClass
    public static void tearDown() {
        benchmarker.tearDown();
    }

    @After
    public void reset() {
        benchmarker.reset();
    }

    @Test
    public void testMultipleStartingPoints() {
        String query = String.format(
                "match\n" +
                        "   $s isa subject, has email \"%s\";\n" +
                        "   $parent isa directory, has path \"%s\";\n" +
                        "   $a1 isa action, has name \"%s\";\n" +
                        "   $a2 isa action, has name \"%s\";\n" +
                        "   $policy (action: $a1, action: $a2) isa segregation-policy;\n" +
                        "   (collection: $parent, member:$o) isa collection-membership;\n" +
                        "   $o has id $oid;" +
                        "   $ac1(object: $o, action: $a1) isa access;\n" +
                        "   $ac2(object: $o, action: $a2) isa access;\n" +
                        "   $p1 (subject: $s, access: $ac1) isa permission;\n" +
                        "   $p2 (subject: $s, access: $ac2) isa permission;\n" +
                        "get $oid;",
                queryParams.segregationEmail, queryParams.segregationObject, queryParams.segregationAction1, queryParams.segregationAction2);
        Benchmark benchmark = new Benchmark("multiple-starting-points", query, 1);
        benchmarker.runBenchmark(benchmark);

        benchmark.assertAnswerCountCorrect();
        benchmark.assertRunningTime(1000);
        benchmark.assertCounters(500, 20, 100, 200);
    }

    @Test
    public void testHighArityBounds() {
        benchmarker.loadSchema("schema_rules_test_specific.tql");
        String query = String.format(
                "match\n" +
                        "   $s isa subject, has email \"%s\";\n" +
                        "   $parent isa directory, has path \"%s\";\n" +
                        "   $a1 isa action, has name \"%s\";\n" +
                        "   $a2 isa action, has name \"%s\";\n" +
                        "   (collection: $parent, member:$o) isa collection-membership;\n" +
                        "   (subject: $s, object: $o, action: $a1, action: $a2) isa high-arity-test-segregation-violation;\n",
                queryParams.segregationEmail, queryParams.segregationObject, queryParams.segregationAction1, queryParams.segregationAction2);

        Benchmark benchmark = new Benchmark("high-arity-bounds", query, 1);
        benchmarker.runBenchmark(benchmark);

        benchmark.assertAnswerCountCorrect();
        benchmark.assertRunningTime(1000);
        benchmark.assertCounters(500, 100, 400, 1500);
    }
}
