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

public class ComplexConjunctionTest extends ReasonerBenchmarkSuite {

    private static final String database = "iam-benchmark-conjunctions";

    public ComplexConjunctionTest() {
        super(database);
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
    public void testCheckPermission() {
        // Simple, but needs to pick the right plan.
        String query = "match\n" +
                "$p isa person, has email \"douglas.schmidt@vaticle.com\";\n" +
                "$f isa file, has path \"root/engineering/typedb-studio/src/README.md\";\n" +
                "$o isa operation, has name \"edit file\";\n" +
                "$a (object: $f, action: $o) isa access;\n" +
                "$pe (subject: $p, access: $a) isa permission, has validity true;";
        Benchmark benchmark = new Benchmark("check-permission", query, 1, 3);
        runBenchmark(benchmark);
        benchmark.assertAnswerCountCorrect();
    }

    @Test
    public void testSegregationViolationMultipleStartingPoints() {
        // Stresses resolvable ordering planner by having many candidate orderings
        String query = "match\n" +
                "   $a1 isa action, has name \"submit pull request\";\n" +
                "   $a2 isa action, has name \"approve pull request\";\n" +
                "   $s isa subject, has email \"genevieve.gallegos@vaticle.com\";\n" +
                "   $parent isa directory, has path \"root/engineering\";\n" +
                "   $policy (action: $a1, action: $a2) isa segregation-policy;\n" +
                "   (collection: $parent, member:$o) isa collection-membership;\n" +
                "   $ac1(object: $o, action: $a1) isa access;\n" +
                "   $ac2(object: $o, action: $a2) isa access;\n" +
                "   $p1(subject: $s, access: $ac1) isa permission;\n" +
                "   $p2(subject: $s, access: $ac2) isa permission;\n";
        Benchmark benchmark = new Benchmark("many-starting-points-segregation-violation", query, 2, 3);
        runBenchmark(benchmark);
        benchmark.assertAnswerCountCorrect();
    }
}
