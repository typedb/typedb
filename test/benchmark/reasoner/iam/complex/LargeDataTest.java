/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner.benchmark.iam.complex;

import com.vaticle.typedb.core.common.diagnostics.Diagnostics;
import com.vaticle.typedb.core.reasoner.benchmark.iam.common.Benchmark;
import com.vaticle.typedb.core.reasoner.benchmark.iam.common.BenchmarkRunner;
import com.vaticle.typedb.core.reasoner.benchmark.iam.common.QueryParams;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class LargeDataTest {

    private static final Path COMMON_RESOURCE_DIR = Paths.get("test", "benchmark", "reasoner", "iam", "resources");
    private static final String database = "iam-benchmark-large-data";
    private static final BenchmarkRunner benchmarker = new BenchmarkRunner(database);
    private final QueryParams queryParams;

    public LargeDataTest() {
        queryParams = QueryParams.load(COMMON_RESOURCE_DIR.resolve("params.yml"));
    }

    @BeforeClass
    public static void setup() throws IOException {
        Diagnostics.Noop.initialise();
        benchmarker.setUp();
        benchmarker.loadDatabase(COMMON_RESOURCE_DIR.resolve("types.tql"), COMMON_RESOURCE_DIR.resolve("data.typedb"));
        benchmarker.loadSchema(COMMON_RESOURCE_DIR.resolve("rules.tql"));
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
    public void testHighSelectivity() {
        String query = "match\n" +
                "   $po (action: $a1, action: $a2) isa segregation-policy;\n" +
                "   $ac1 (object: $o, action: $a1) isa access;\n" +
                "   $ac2 (object: $o, action: $a2) isa access;\n" +
                "   $p1 (subject: $s, access: $ac1) isa permission;\n" +
                "   $p2 (subject: $s, access: $ac2) isa permission;\n" +
                "get;";
        Benchmark benchmark = new Benchmark("high-selectivity", query, 4);
        benchmarker.runBenchmark(benchmark);

        benchmark.assertAnswerCountCorrect();
        benchmark.assertRunningTime(1500);
        benchmark.assertCounters(500, 36, 250, 303, 575);
    }

    @Test
    public void testCombinatorialResults() {
        String query = "match\n" +
                "   $c isa collection-membership;\n" +
                "   $s isa set-membership;\n" +
                "   $g isa group-membership;\n" +
                "get;";
        Benchmark benchmark = new Benchmark("combinatorial-results", query, 133000);
        benchmarker.runBenchmark(benchmark);

        benchmark.assertAnswerCountCorrect();
        benchmark.assertRunningTime(5000);
        benchmark.assertCounters(200, 128, 4, 5, 138720);
    }

    @Test
    public void testLargeNegation() {
        String query = String.format(
                "match\n" +
                        "   $p isa person, has email \"%s\";\n" +
                        "   $o isa object, has id $oid;\n" +
                        "   $a isa action, has name $aid;\n" +
                        "   $ac (object: $o, action: $a) isa access;\n" +
                        "   $pe (subject: $p, access: $ac) isa permission;\n" +
                        "   not {\n" +
                        "           $pe-other (subject: $other, access: $ac) isa permission;\n" +
                        "           not { $other is $p; };\n" +
                        "           $p has email $email; # just to bind $p\n" +
                        " };\n" +
                        "get $oid, $aid;",
                queryParams.largeNegationEmail);
        Benchmark benchmark = new Benchmark("large-negation", query, 1);
        benchmarker.runBenchmark(benchmark);

        benchmark.assertAnswerCountCorrect();
        benchmark.assertRunningTime(2500);
        benchmark.assertCounters(500, 500, 1000, 1750, 11750);
    }
}
