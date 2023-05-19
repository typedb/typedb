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

public class RealQueriesTest {

    private static final String database = "iam-benchmark-real-queries";
    private static final BenchmarkRunner benchmarker = new BenchmarkRunner(database);
    private final QueryParams queryParams;

    public RealQueriesTest() {
        queryParams = QueryParams.load();
    }

    @BeforeClass
    public static void setup() throws IOException {
        benchmarker.setUp();
        benchmarker.loadSchema("schema_types.tql");
        benchmarker.loadSchema("schema_rules_optimised.tql");
        benchmarker.importData("data.typedb");
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
    public void testCheckPermission() {
        String query = String.format(
                "match\n" +
                        "$p isa person, has email \"%s\";\n" +
                        "$f isa file, has path \"%s\";\n" +
                        "$o isa operation, has name \"%s\";\n" +
                        "$a (object: $f, action: $o) isa access;\n" +
                        "$pe (subject: $p, access: $a) isa permission, has validity true;\n",
                queryParams.permissionEmail, queryParams.permissionObject, queryParams.permissionAction);
        Benchmark benchmark = new Benchmark("check-permission", query, 1);
        benchmarker.runBenchmark(benchmark);

        benchmark.assertAnswerCountCorrect();
        benchmark.assertRunningTime(1000);
        benchmark.assertCounters(500, 185, 233, 443);
    }

    @Test
    public void testListSubjectPermissions() {
        String query = String.format(
                "match\n" +
                        "$p isa person, has email \"%s\";\n" +
                        "$o isa object, has id $id;\n" +
                        "$a isa action, has name $n;\n" +
                        "$ac (object: $o, action: $a) isa access;\n" +
                        "$pe (subject: $p, access: $ac) isa permission, has validity $v;\n",
                queryParams.permissionEmail);
        Benchmark benchmark = new Benchmark("list-subject-permissions", query, 67);
        benchmarker.runBenchmark(benchmark);

        benchmark.assertAnswerCountCorrect();
        benchmark.assertRunningTime(1500);
        benchmark.assertCounters(500, 251, 448, 740);
    }

    @Test
    public void testListSegregationViolations() {
        String query = "match\n" +
                "$s isa subject, has id $s-id;\n" +
                "$o isa object, has id $o-id;\n" +
                "$p isa segregation-policy, has name $n;\n" +
                "(subject: $s, object: $o, policy: $p) isa segregation-violation;\n";
        Benchmark benchmark = new Benchmark("list-segregation-violations", query, 1);
        benchmarker.runBenchmark(benchmark);

        benchmark.assertAnswerCountCorrect();
        benchmark.assertRunningTime(500);
        benchmark.assertCounters(200, 37, 251, 507);
    }

    @Test
    public void testListChangeRequests() {
        String query = "match\n" +
                "\n" +
                "$o isa object, has parent-company-name \"Vaticle\", has id $oid;\n" +
                "$a isa action, has parent-company-name \"Vaticle\", has name $a-name ;\n" +
                "$ac(object: $o, action: $a) isa access;\n" +
                "$s-requesting isa subject, has parent-company-name \"Vaticle\", has id $s-requesting-id;\n" +
                "$s-requested isa subject, has parent-company-name \"Vaticle\", has id $s-requested-id;\n" +
                "(requester: $s-requesting, requestee: $s-requested, change: $ac) isa change-request;\n";
        Benchmark benchmark = new Benchmark("list-change-requests", query, 0); // TODO: Generate and keep in dataset
        benchmarker.runBenchmark(benchmark);

        benchmark.assertAnswerCountCorrect();
        benchmark.assertRunningTime(500);
        benchmark.assertCounters(200, 37, 251, 507);
    }
}
