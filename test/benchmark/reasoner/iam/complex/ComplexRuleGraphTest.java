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

public class ComplexRuleGraphTest {
    static final Path RESOURCE_DIRECTORY = Paths.get("test", "benchmark", "reasoner", "iam", "complex");
    private static final Path COMMON_RESOURCE_DIR = Paths.get("test", "benchmark", "reasoner", "iam", "resources");

    private static final String database = "iam-benchmark-rules";
    private static final BenchmarkRunner runner = new BenchmarkRunner(database);
    private final QueryParams queryParams;

    public ComplexRuleGraphTest() {
        queryParams = QueryParams.load(COMMON_RESOURCE_DIR.resolve("params.yml"));
    }

    @BeforeClass
    public static void setup() throws IOException {
        Diagnostics.Noop.initialise();
        runner.setUp();
        runner.loadDatabase(COMMON_RESOURCE_DIR.resolve("types.tql"), COMMON_RESOURCE_DIR.resolve("data.typedb"));
        runner.loadSchema(RESOURCE_DIRECTORY.resolve("complex-rule-graph-test.tql"));
        runner.warmUp();
    }

    @AfterClass
    public static void tearDown() {
        runner.tearDown();
    }

    @After
    public void reset() {
        runner.reset();
    }

    @Test
    public void testCombinatorialProofsSingle() {
        String query = String.format(
                "match\n" +
                        "$p isa person, has email \"%s\";\n" +
                        "$f isa file, has path \"%s\";\n" +
                        "$o isa operation, has name \"%s\";\n" +
                        "$a (object: $f, action: $o) isa access;\n" +
                        "$pe (subject: $p, access: $a) isa permission;\n" +
                "get;",
                queryParams.permissionEmail, queryParams.permissionObject, queryParams.permissionAction);
        Benchmark benchmark = new Benchmark("combinatorial-proofs-single", query, 1);
        runner.runBenchmark(benchmark);
        benchmark.assertAnswerCountCorrect();
        benchmark.assertRunningTime(1000);
        benchmark.assertCounters(200, 149, 301, 301, 2150);
    }

    @Test
    public void testCombinatorialProofsAll() {
        String query = String.format(
                "match\n" +
                        "$p isa person, has email \"%s\";\n" +
                        "$o isa object, has id $o-id;\n" +
                        "$a isa action, has name $an;\n" +
                        "$ac (object: $o, action: $a) isa access;\n" +
                        "$pe (subject: $p, access: $ac) isa permission;\n" +
                "get;",
                queryParams.permissionEmail);

        Benchmark benchmark = new Benchmark("combinatorial-proofs-all", query, 67);
        runner.runBenchmark(benchmark);
        benchmark.assertAnswerCountCorrect();
        benchmark.assertRunningTime(1000);
        benchmark.assertCounters(200, 265, 342, 343, 7894);
    }
}
