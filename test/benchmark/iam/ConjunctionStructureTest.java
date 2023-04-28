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
    private static final Benchmark.CSVResults printTo = new Benchmark.CSVResults(null);
    private static final BenchmarkRunner benchmarker = new BenchmarkRunner(database);

    public ConjunctionStructureTest() { }

    @BeforeClass
    public static void setup() throws IOException {
        benchmarker.setUp();
        benchmarker.loadSchema("schema_types.tql");
        benchmarker.loadSchema("schema_rules_optimised.tql");
        benchmarker.loadData("data.typedb");
    }

    @AfterClass
    public static void tearDown() {
        if (printTo != null) printTo.flush();
        benchmarker.tearDown();
    }

    @After
    public void reset() {
        benchmarker.reset();
    }

    @Test
    public void testMultipleStartingPoints() {
        String query = "match\n" +
                "   $a1 isa action, has name \"submit pull request\";\n" +
                "   $a2 isa action, has name \"approve pull request\";\n" +
                "   $s isa subject, has email \"genevieve.gallegos@vaticle.com\";\n" +
                "   $parent isa directory, has path \"root/engineering\";\n" +
                "   $policy (action: $a1, action: $a2) isa segregation-policy;\n" +
                "   (collection: $parent, member:$o) isa collection-membership;\n" +
                "   $o has id $oid;" +
                "   $ac1(object: $o, action: $a1) isa access;\n" +
                "   $ac2(object: $o, action: $a2) isa access;\n" +
                "   $p1 (subject: $s, access: $ac1) isa permission;\n" +
                "   $p2 (subject: $s, access: $ac2) isa permission;\n" +
                "get $oid;";
        Benchmark benchmark = new Benchmark("multiple-starting-points", query, 1);
        benchmarker.runBenchmark(benchmark);
        benchmark.assertAnswerCountCorrect();
        benchmark.mayPrintResults(printTo);
    }

    @Test
    public void testHighArityBounds() {
        benchmarker.loadSchema("schema_rules_test_specific.tql");
        String query = "match\n" +
                "   $a1 isa action, has name \"submit pull request\";\n" +
                "   $a2 isa action, has name \"approve pull request\";\n" +
                "   $s isa subject, has email \"genevieve.gallegos@vaticle.com\";\n" +
                "   $parent isa directory, has path \"root/engineering\";\n" +
                "   (collection: $parent, member:$o) isa collection-membership;\n" +
                "   (subject: $s, object: $o, action: $a1, action: $a2) isa high-arity-test-segregation-violation;\n";
        Benchmark benchmark = new Benchmark("high-arity-bounds", query, 1);
        benchmarker.runBenchmark(benchmark);
        benchmark.assertAnswerCountCorrect();
        benchmark.mayPrintResults(printTo);
    }
}
