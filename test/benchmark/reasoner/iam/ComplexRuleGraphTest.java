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

import com.vaticle.typedb.core.reasoner.benchmark.iam.common.Benchmark;
import com.vaticle.typedb.core.reasoner.benchmark.iam.common.BenchmarkRunner;
import com.vaticle.typedb.core.reasoner.benchmark.iam.common.QueryParams;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class ComplexRuleGraphTest {

    private static final String database = "iam-benchmark-rules";
    private static final BenchmarkRunner benchmarker = new BenchmarkRunner(database);
    private final QueryParams queryParams;

    public ComplexRuleGraphTest() {
        queryParams = QueryParams.load();
    }

    @BeforeClass
    public static void setup() throws IOException {
        benchmarker.setUp();
        benchmarker.loadSchema("schema_types.tql");
        benchmarker.loadSchema("schema_rules_naive.tql");
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
    public void testCombinatorialProofsSingle() {
        String query = String.format(
                "match\n" +
                        "$p isa person, has email \"%s\";\n" +
                        "$f isa file, has path \"%s\";\n" +
                        "$o isa operation, has name \"%s\";\n" +
                        "$a (object: $f, action: $o) isa access;\n" +
                        "$pe (subject: $p, access: $a) isa permission;\n",
                queryParams.permissionEmail, queryParams.permissionObject, queryParams.permissionAction);
        Benchmark benchmark = new Benchmark("combinatorial-proofs-single", query, 1);
        benchmarker.runBenchmark(benchmark);
        benchmark.assertAnswerCountCorrect();
        benchmark.assertRunningTime(1000);
        benchmark.assertCounters(200, 149, 301, 1658);
    }

    @Test
    public void testCombinatorialProofsAll() {
        String query = String.format(
                "match\n" +
                        "$p isa person, has email \"%s\";\n" +
                        "$o isa object, has id $o-id;\n" +
                        "$a isa action, has name $an;\n" +
                        "$ac (object: $o, action: $a) isa access;\n" +
                        "$pe (subject: $p, access: $ac) isa permission;\n",
                queryParams.permissionEmail);

        Benchmark benchmark = new Benchmark("combinatorial-proofs-all", query, 67);
        benchmarker.runBenchmark(benchmark);
        benchmark.assertAnswerCountCorrect();
        benchmark.assertRunningTime(1000);
        benchmark.assertCounters(100, 265, 342, 837);
    }
}
