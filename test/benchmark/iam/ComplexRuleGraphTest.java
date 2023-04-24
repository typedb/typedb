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
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ComplexRuleGraphTest {

    private static final Benchmark.CSVResults printTo = new Benchmark.CSVResults(null);
    private static final String database = "iam-benchmark-rules";
    private final BenchmarkRunner benchmarker;

    public ComplexRuleGraphTest() {
        benchmarker = new BenchmarkRunner(database);
    }

    @AfterClass
    public static void mayPrintResults() {
        if (printTo != null) printTo.flush();
    }

    @Before
    public void setUp() throws IOException {
        benchmarker.setUp();
        benchmarker.loadSchema("schema_types.tql");
        benchmarker.loadSchema("schema_rules_naive.tql");
        benchmarker.loadData("data_small.typedb");
    }

    @After
    public void tearDown() {
        benchmarker.tearDown();
    }

    @Test
    public void testCombinatorialProofsSingle() {
        String query = "match\n" +
                "$p isa person, has email \"douglas.schmidt@vaticle.com\";\n" +
                "$f isa file, has path \"root/engineering/typedb-studio/src/README.md\";\n" +
                "$o isa operation, has name \"edit file\";\n" +
                "$a (object: $f, action: $o) isa access;\n" +
                "$pe (subject: $p, access: $a) isa permission;\n";
        Benchmark benchmark = new Benchmark("combinatorial-proofs-single", query, 1);
        benchmarker.runBenchmark(benchmark);
        benchmark.assertAnswerCountCorrect();
        benchmark.mayPrintResults(printTo);
    }

    @Test
    public void testCombinatorialProofsAll() {
        String query = "match\n" +
                "$p isa person, has email \"douglas.schmidt@vaticle.com\";\n" +
                "$o isa object, has id $o-id;\n" +
                "$a isa action, has name $an;\n" +
                "$ac (object: $o, action: $a) isa access;\n" +
                "$pe (subject: $p, access: $ac) isa permission;\n";
        Benchmark benchmark = new Benchmark("combinatorial-proofs-all", query, 67);
        benchmarker.runBenchmark(benchmark);
        benchmark.assertAnswerCountCorrect();
        benchmark.mayPrintResults(printTo);
    }
}
