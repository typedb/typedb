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
package com.vaticle.typedb.core.reasoner.planner;

import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typedb.core.database.CoreSession;
import com.vaticle.typedb.core.database.CoreTransaction;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.variable.Reference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class RecursivePorPlannerTest {

    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("recursive-por-planner-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
            .storageDataCacheSize(MB).storageIndexCacheSize(MB);
    private static final String database = "recursive-por-planner-test";
    private static CoreDatabaseManager databaseMgr;
    private static CoreSession session;
    private static CoreTransaction transaction;

    @Before
    public void setUp() throws IOException {
        Util.resetDirectory(dataDir);
        databaseMgr = CoreDatabaseManager.open(options);
        databaseMgr.create(database);
        initialise(Arguments.Session.Type.SCHEMA, Arguments.Transaction.Type.WRITE);

        transaction.query().define(TypeQL.parseQuery("define " +
                "person sub entity, owns first-name, owns last-name," +
                "   plays friendship:friendor, plays friendship:friendee;" +
                "man sub person;" +
                "name sub attribute, abstract, value string;" +
                "first-name sub name;" +
                "last-name sub name;" +
                "household sub relation, relates member;" +
                "person plays household:member;" +
                "friendship sub relation, relates friendor, relates friendee;"));
        transaction.commit();
        session.close();

        initialise(Arguments.Session.Type.DATA, Arguments.Transaction.Type.WRITE);
        transaction.query().insert(TypeQL.parseQuery("insert " +
                "$p1 isa person, has first-name \"p1_f\", has last-name \"p1_l\";" +
                "$p2 isa person;" +
                "$p3 isa person;" +
                "$m1 isa man, has first-name \"m1_f\", has last-name \"m1_l\";" +
                "$m2 isa man, has first-name \"m2_f\";" +

                "(member: $p1, member: $p2, member: $p3) isa household;" +
                "(member: $m1) isa household;" +

                "(friendor: $p1, friendee: $p2) isa friendship;" +
                "(friendor: $p2, friendee: $p3) isa friendship;" +
                "(friendor: $m1, friendee: $m2) isa friendship;"));
        transaction.commit();
    }

    private void initialise(Arguments.Session.Type schema, Arguments.Transaction.Type write) {
        session = databaseMgr.session(database, schema);
        transaction = session.transaction(write);
    }

    @After
    public void tearDown() {
        transaction.close();
        session.close();
        databaseMgr.close();
    }

    @Test
    public void test_single_retrieval() {
        initialise(Arguments.Session.Type.DATA, Arguments.Transaction.Type.READ);
        RecursivePorPlanner planSpaceSearch = new RecursivePorPlanner(transaction.traversal(), transaction.concepts(), transaction.logic());
        ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ $p isa person; }", transaction.logic()));
        planSpaceSearch.plan(conjunction, set());
        ReasonerPlanner.Plan plan = planSpaceSearch.getPlan(conjunction, set());
        assertEquals(5L, plan.cost()); // For now the retrieval cost is just the answer-count
    }

    @Test
    public void test_owns_retrieval() {
        // Still just answer counts. Improve if we have an improved estimate for retrievables
        initialise(Arguments.Session.Type.DATA, Arguments.Transaction.Type.READ);

        RecursivePorPlanner planSpaceSearch = new RecursivePorPlanner(transaction.traversal(), transaction.concepts(), transaction.logic());
        {   // Query only count of $p, where $p isa man;
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ $p isa man, has name $n; }", transaction.logic()));
            planSpaceSearch.plan(conjunction, set());
            ReasonerPlanner.Plan plan = planSpaceSearch.getPlan(conjunction, set());
            assertEquals(3L, plan.cost());
        }

        {   // Query count of both variables $p and $n, where $p isa! person
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ $p isa! person, has name $n; }", transaction.logic()));
            planSpaceSearch.plan(conjunction, set());
            ReasonerPlanner.Plan plan = planSpaceSearch.getPlan(conjunction, set());
            assertEquals(2L, plan.cost());
        }

        {   // Query count of both variables $p and $n, where $p isa person (and subtypes)
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ $p isa person, has name $n; }", transaction.logic()));
            planSpaceSearch.plan(conjunction, set());
            ReasonerPlanner.Plan plan = planSpaceSearch.getPlan(conjunction, set());
            assertEquals(5L, plan.cost());
        }
    }

    @Test
    public void test_with_concludable_and_retrievable() {
        initialise(Arguments.Session.Type.SCHEMA, Arguments.Transaction.Type.WRITE);
        transaction.query().define(TypeQL.parseQuery("define " +
                "rule everyone-is-named-dave: " +
                "when { $x isa person; } " +
                "then { $x has first-name \"Dave\"; };"));
        transaction.commit();
        session.close();

        initialise(Arguments.Session.Type.DATA, Arguments.Transaction.Type.READ);
        RecursivePorPlanner planSpaceSearch = new RecursivePorPlanner(transaction.traversal(), transaction.concepts(), transaction.logic());
        {
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ $p has name $n; }", transaction.logic()));
            planSpaceSearch.plan(conjunction, set());
            ReasonerPlanner.Plan plan = planSpaceSearch.getPlan(conjunction, set());
            assertEquals(15L, plan.cost()); // = (10)localCost + (5)costOfRule = 15
        }

        {
            // Plan: {$p has name $n} -> { (...) isa friendship;} } :  1 * (10 + 5) + min(1,5/3) * 3 = 18
            // Plan: { (...) isa friendship;} -> {$p has name $n} } :  1 * 3  + min(1, 3/5) * (10 + 5) = 12
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ $p has name $n; (friendor: $p, friendee: $f) isa friendship; }", transaction.logic()));
            planSpaceSearch.plan(conjunction, set());
            ReasonerPlanner.Plan plan = planSpaceSearch.getPlan(conjunction, set());
            assertEquals(12L, plan.cost());
        }
    }

    @Test
    public void test_cycles() {
        initialise(Arguments.Session.Type.SCHEMA, Arguments.Transaction.Type.WRITE);
        transaction.query().define(TypeQL.parseQuery("define " +
                "transitive-friendship sub relation, relates friendor, relates friendee;" +
                "person plays transitive-friendship:friendor, plays transitive-friendship:friendee;" +

                "rule friendship-is-transitive-1: " +
                "when { (friendor: $f1, friendee: $f2) isa friendship; } " +
                "then { (friendor: $f1, friendee: $f2) isa transitive-friendship; };" +

                "rule friendship-is-transitive-2: " +
                "when { (friendor: $f1, friendee: $f2) isa friendship; (friendor: $f2, friendee: $f3) isa transitive-friendship;  } " +
                "then { (friendor: $f1, friendee: $f3) isa transitive-friendship; };" +
                ""));
        transaction.commit();
        session.close();

        initialise(Arguments.Session.Type.DATA, Arguments.Transaction.Type.READ);
        RecursivePorPlanner planSpaceSearch = new RecursivePorPlanner(transaction.traversal(), transaction.concepts(), transaction.logic());
        {
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{(friendor: $x, friendee: $y) isa transitive-friendship; }", transaction.logic()));
            planSpaceSearch.plan(conjunction, set());
            ReasonerPlanner.Plan plan = planSpaceSearch.getPlan(conjunction, set());
            assertEquals(45L, plan.cost());
            // Answercount($x,$y) = 18
            // Answercount($x) = Answercount($y) = 5
            // Cost = (18) query + (3) rule1{} + (24) rule2{}
            // rule1{}: Acyclic-cost = 3 regardless of plan, (Acyclic -> added into the acyclic costs of callers)
            // rule2{}:
            //      Plan: {(...) isa friendship;} -> {(...) isa transitive-friendship;}
            //          Acyclic-cost : 1 * 3 + (3/5) * 21 = 15.6;
            //          CyclicConcludables -> ScalingFactor: { rule2{$f1}: : 3/5}
            //      Plan: {(...) isa transitive-friendship;} -> {(...) isa friendship;}
            //          Acyclic-cost : 1 * 21 + min(1, 5/3) * 3 = 24
            //          CyclicConcludables -> ScalingFactor: { rule2{}: 1 }
            //      Both candidate orderings)
            //      (rule2 used to be 21 when we didn't discern between acyclic/cyclic dependencies

        }
    }

    @Test
    public void test_cycles_same_type_transitive() {
        initialise(Arguments.Session.Type.SCHEMA, Arguments.Transaction.Type.WRITE);
        transaction.query().define(TypeQL.parseQuery("define " +
                "rule friendship-iteself-is-transitive: " +
                "when { (friendor: $f1, friendee: $f2) isa friendship; (friendor: $f2, friendee: $f3) isa friendship;  } " +
                "then { (friendor: $f1, friendee: $f3) isa friendship; };"));
        transaction.commit();
        session.close();

        initialise(Arguments.Session.Type.DATA, Arguments.Transaction.Type.READ);
        RecursivePorPlanner planSpaceSearch = new RecursivePorPlanner(transaction.traversal(), transaction.concepts(), transaction.logic());
        {
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{(friendor: $x, friendee: $y) isa friendship; }", transaction.logic()));

            planSpaceSearch.plan(conjunction, set());
            ReasonerPlanner.Plan plan = planSpaceSearch.getPlan(conjunction, set());
            assertEquals(125L, plan.cost());
            // LocalCost = AnswerCount( $_0, $x, $y ) = 25
            // AnswerCount($x) = AnswerCount($y) = 5
            // Cost = (25 + 1 * rule{} + * 5/5 rule{f1}) = 25 + 1 * 50 + 5/5 * 50 = 125     [ = (25 + 1 * rule{} + 5/5 * rule{f2}) ]
            // rule{}:
            //      Plan: f12 -> f23
            //          AcyclicCost: 1 * 25 + 5/5 * 25 = 50
            //          CyclicConcludables: { rule{}: 1, rule{f1}: 1 }
            //      Plan: f23 -> f12
            //          AcyclicCost: 1 * 25 + 5/5 * 25 = 50
            //          CyclicConcludables: { rule{}: 1, rule{f2}: 1 }
            // rule{f1}:
            //      Plan: f12 -> f23
            //          AcyclicCost: 1 * 25 + 5/5 * 25 = 50
            //          CyclicConcludables: { rule{f1}: 1}
            // rule{f2}:
            //      Plan: f23 -> f12
            //          AcyclicCost: 1 * 25 + 5/5 * 25 = 50
            //          CyclicConcludables: { rule{f2}: 1}
        }
    }

    @Test
    public void test_nested_cycles() {
        // the example may not make sense.
        initialise(Arguments.Session.Type.SCHEMA, Arguments.Transaction.Type.WRITE);
        transaction.query().define(TypeQL.parseQuery("define " +
                "transitive-friendship sub relation, relates friendor, relates friendee;" +
                "person plays transitive-friendship:friendor, plays transitive-friendship:friendee;" +

                "rule friendship-is-transitive-1: " +
                "when { (friendor: $f1, friendee: $f2) isa friendship; } " +
                "then { (friendor: $f1, friendee: $f2) isa transitive-friendship; };" +

                "rule friendship-is-transitive-2: " +
                "when { (friendor: $f1, friendee: $f2) isa friendship; (friendor: $f2, friendee: $f3) isa transitive-friendship;  } " +
                "then { (friendor: $f1, friendee: $f3) isa transitive-friendship; };"));

        transaction.query().define(TypeQL.parseQuery("define " +
                "can-live-with sub relation, relates guest, relates host;" +
                "person plays can-live-with:guest, plays can-live-with:host;" +

                "rule clw-if-housemates: " +
                "when { (member: $f1, member: $f2) isa household ; } " +
                "then { (guest: $f1, host: $f2) isa can-live-with; };" +

                "rule clw-if-housemate-friends-with: " +
                "when { (guest: $f1, host: $f2) isa can-live-with; (friendor: $f2, friendee: $f3) isa transitive-friendship;  } " +
                "then { (guest: $f1, host: $f3) isa can-live-with; };"));

        transaction.query().define(TypeQL.parseQuery("define " +
                "rule clw-means-friends: " +
                "when { (guest: $f1, host: $f2) isa can-live-with; } " +
                "then { (friendor: $f1, friendee: $f2) isa transitive-friendship; };"));
        transaction.commit();
        session.close();

        initialise(Arguments.Session.Type.DATA, Arguments.Transaction.Type.READ);
        RecursivePorPlanner planSpaceSearch = new RecursivePorPlanner(transaction.traversal(), transaction.concepts(), transaction.logic());
        {
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{(friendor: $x, friendee: $y) isa transitive-friendship; }", transaction.logic()));
            planSpaceSearch.plan(conjunction, set());
            ReasonerPlanner.Plan plan = planSpaceSearch.getPlan(conjunction, set());
            assertTrue(plan.cost() > 0); // TODO: Verify whether this is the right/sensible answer
        }

        {
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{(guest: $x, host: $y) isa can-live-with; }", transaction.logic()));
            planSpaceSearch.plan(conjunction, set());
            ReasonerPlanner.Plan plan = planSpaceSearch.getPlan(conjunction, set());
            assertTrue(plan.cost() > 0); // TODO: Verify whether this is the right/sensible answer
        }
    }

    @Test
    public void test_bound_cycles() {
        initialise(Arguments.Session.Type.SCHEMA, Arguments.Transaction.Type.WRITE);
        transaction.query().define(TypeQL.parseQuery("define " +
                "transitive-friendship sub relation, relates friendor, relates friendee;" +
                "person plays transitive-friendship:friendor, plays transitive-friendship:friendee;" +

                "rule friendship-is-transitive-1: " +
                "when { (friendor: $f1, friendee: $f2) isa friendship; } " +
                "then { (friendor: $f1, friendee: $f2) isa transitive-friendship; };" +

                "rule friendship-is-transitive-2: " +
                "when { (friendor: $f1, friendee: $f2) isa friendship; (friendor: $f2, friendee: $f3) isa transitive-friendship;  } " +
                "then { (friendor: $f1, friendee: $f3) isa transitive-friendship; };" +
                ""));
        transaction.commit();
        session.close();

        initialise(Arguments.Session.Type.DATA, Arguments.Transaction.Type.READ);
        RecursivePorPlanner planSpaceSearch = new RecursivePorPlanner(transaction.traversal(), transaction.concepts(), transaction.logic());
        {
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{$x has name \"Jim\"; (friendor: $x, friendee: $y) isa transitive-friendship; }", transaction.logic()));
            planSpaceSearch.plan(conjunction, set());
            ReasonerPlanner.Plan plan = planSpaceSearch.getPlan(conjunction, set());
            assertEquals(26L, plan.cost());
            // Answercount($x,$y) = 5
            // Answercount($x) = 1; Answercount($y) = 5
            // Cost = 26 = (5) query + 1/5 * (3) rule1{$x:1} + min(1, (1+1/5) ) * (21) rule2{$x:1}
            // With plan(rule2{$x}) = { (...) isa friendship; } -> { (...) isa transitive-friendship; }
        }

        {
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{$y has name \"Jim\"; (friendor: $x, friendee: $y) isa transitive-friendship; }", transaction.logic()));
            planSpaceSearch.plan(conjunction, set());
            ReasonerPlanner.Plan plan = planSpaceSearch.getPlan(conjunction, set());
            assertEquals(10, plan.cost());
            // Answercount($x,$y) = 5
            // Answercount($x) = 3; Answercount($y) = 1
            // Cost = 15 = (8) query + min(1,5/3) * (3) rule1{$y:1} + (0 + 1/5) * (21) rule2{$y:1}
            // With plan(rule2{$x}) = { (...) isa transitive-friendship; } -> { (...) isa friendship; }
        }
    }

    private static Conjunction resolvedConjunction(String query, LogicManager logicMgr) {
        Disjunction disjunction = resolvedDisjunction(query, logicMgr);
        assert disjunction.conjunctions().size() == 1;
        return disjunction.conjunctions().get(0);
    }

    private static Disjunction resolvedDisjunction(String query, LogicManager logicMgr) {
        Disjunction disjunction = Disjunction.create(TypeQL.parsePattern(query).asConjunction().normalise());
        logicMgr.typeInference().applyCombination(disjunction);
        return disjunction;
    }
}
