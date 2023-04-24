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

public class LanguageFeatureTest{

    private static final Benchmark.CSVResults printTo = new Benchmark.CSVResults(null);
    private static final String database = "iam-benchmark-language-features";
    private final BenchmarkRunner benchmarker;

    public LanguageFeatureTest() {
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
        benchmarker.loadSchema("schema_rules_optimised.tql");
        benchmarker.loadData("data_small.typedb");
    }

    @After
    public void tearDown() {
        benchmarker.tearDown();
    }

    @Test
    public void testBoundRelation() {
        String query = "match\n" +
                "$p isa person, has email \"douglas.schmidt@vaticle.com\";\n" +
                "$f isa file, has path \"root/engineering/typedb-studio/src/README.md\";\n" +
                "$o isa operation, has name \"edit file\";\n" +
                "$a (object: $f, action: $o) isa access;\n" +
                "$pe isa permission;\n" +
                "$pe (subject: $p, access: $a) isa permission;\n" +
                "$pe is $pe-same;\n" +
                "$pe-same (subject: $other-p, access: $other-a) isa permission;\n";
        Benchmark benchmark = new Benchmark("bound-relation", query, 1);
        benchmarker.runBenchmark(benchmark);
        benchmark.assertAnswerCountCorrect();
    }

    @Test
    public void testValuePredicateFiltering() {
        String query = "match\n" +
                "$p isa person, has $email; $email = \"douglas.schmidt@vaticle.com\";\n" +
                "$f isa file, has $path; $path = \"root/engineering/typedb-studio/src/README.md\";\n" +
                "$o isa operation, has $operation; $operation = \"edit file\";\n" +
                "$a (object: $f, action: $o) isa access;\n" +
                "$pe (subject: $p, access: $a) isa permission, has validity true;\n";
        Benchmark benchmark = new Benchmark("value-predicate-filtering", query, 1);
        benchmarker.runBenchmark(benchmark);
        benchmark.assertAnswerCountCorrect();
    }

    @Test
    public void variabilisedRules() {
        benchmarker.loadSchema("schema_rules_test_specific.tql");
        String query = "match\n" +
                "$p isa person, has email \"douglas.schmidt@vaticle.com\";\n" +
                "(group: $g, member: $p) isa variabilised-group-membership;\n";
        Benchmark benchmark = new Benchmark("variabilised-rules", query, 3);
        benchmarker.runBenchmark(benchmark);
        benchmark.assertAnswerCountCorrect();
    }
}
