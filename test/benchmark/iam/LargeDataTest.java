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
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class LargeDataTest extends ReasonerBenchmarkSuite {

    private static final String database = "iam-benchmark-data";

    LargeDataTest() {
        this(false);
    }

    LargeDataTest(boolean collectResults) {
        super(database, collectResults);
    }

    @Before
    public void setUp() throws IOException {
        benchmarker.setUp();
        benchmarker.loadSchema("schema_types.tql");
        benchmarker.loadData("data_small.typedb");
    }

    @After
    public void tearDown() {
        benchmarker.tearDown();
    }

    @Test
    public void testSegregationViolationOptimised() {
        String query = "match\n" +
                "   (subject: $s, object: $o, policy: $po) isa segregation-violation;\n";
        benchmarker.loadSchema("schema_rules_optimised.tql");
        Benchmark benchmark = new Benchmark("segregation-violation-optimised", query, 1, 3);
        runBenchmark(benchmark);
        benchmark.assertAnswerCountCorrect();
    }
}
