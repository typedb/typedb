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
import com.vaticle.typedb.core.common.parameters.Options.Database;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typedb.core.database.CoreSession;
import com.vaticle.typedb.core.database.CoreTransaction;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
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

public class CostEstimatorTest {

    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("resolver-manager-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Database options = new Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
            .storageDataCacheSize(MB).storageIndexCacheSize(MB);
    private static final String database = "resolver-manager-test";
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
    public void test_type_based_estimate_with_subtypes() {
        initialise(Arguments.Session.Type.DATA, Arguments.Transaction.Type.READ);

        CostEstimator costEstimator = new CostEstimator(transaction.logic());
        ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ $p isa person; }", transaction.logic()));

        long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("p")));
        assertEquals(5L, cost);
    }

    @Test
    public void test_type_based_estimate_direct_type() {
        initialise(Arguments.Session.Type.DATA, Arguments.Transaction.Type.READ);

        CostEstimator costEstimator = new CostEstimator(transaction.logic());
        ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ $p isa! person; }", transaction.logic()));

        long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("p")));
        assertEquals(3L, cost);
    }

    @Test
    public void test_owns_based_estimate() {
        initialise(Arguments.Session.Type.DATA, Arguments.Transaction.Type.READ);

        CostEstimator costEstimator = new CostEstimator(transaction.logic());
        {   // Query only count of $p, where $p isa man;
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ $p isa man, has name $n; }", transaction.logic()));
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("p")));
            assertEquals(2L, cost);
        }

        {   // Query only count of $p and $n, where $p isa man
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ $p isa man, has name $n; }", transaction.logic()));
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("p", "n")));
            assertEquals(3L, cost);
        }

        {   // Query count of both variables $p and $n, where $p isa! person
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ $p isa! person, has name $n; }", transaction.logic()));
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("p", "n")));
            assertEquals(2L, cost);
        }

        {   // Query count of both variables $p and $n, where $p isa person (and subtypes)
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ $p isa person, has name $n; }", transaction.logic()));
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("p", "n")));
            assertEquals(5L, cost);
        }
    }

    @Test
    public void test_owns_based_estimate_two_concludables() {
        initialise(Arguments.Session.Type.DATA, Arguments.Transaction.Type.READ);

        CostEstimator costEstimator = new CostEstimator(transaction.logic());
        {
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ $m isa man, has name $n; $p isa! person, has $n; }", transaction.logic()));
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("n")));
            assertEquals(2L, cost);
        }

        {   // Restrict types
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ $m isa man, has  first-name $n; $p isa! person, has $n; }", transaction.logic()));
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("n")));
            assertEquals(1L, cost);
        }

        {
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ $m isa man, has name $n; $p isa! person, has $n; }", transaction.logic()));
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("m", "p")));
            assertEquals(4L, cost);
        }

        {
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ $m isa man, has first-name $n; $p isa! person, has $n; }", transaction.logic()));
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("m", "p")));
            assertEquals(2L, cost);
        }
    }

    @Test
    public void test_roles_based_estimate() {
        initialise(Arguments.Session.Type.DATA, Arguments.Transaction.Type.READ);

        CostEstimator costEstimator = new CostEstimator(transaction.logic());
        {   // Query a single role-player
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ ($p1) isa household; }", transaction.logic()));
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("p1")));
            assertEquals(4L, cost);
        }

        {   // Two role-players, projected to one
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ ($p1, $p2) isa household; }", transaction.logic()));
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("p1")));
            assertEquals(4L, cost);
        }

        {   // Just the relation
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ $r isa household; }", transaction.logic()));
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("r")));
            assertEquals(2L, cost);
        }

        {   // Project to just the relation
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ $r ($p1, $p2) isa household; }", transaction.logic()));
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("r")));
            assertEquals(2L, cost);
        }

        {   // Relation and one role-player, binary constraints are considered.
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ $r ($p1) isa household; }", transaction.logic()));
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("r", "p1")));
            assertEquals(4L, cost);
        }

        {   // Two role-players
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ ($p1, $p2) isa household; }", transaction.logic()));
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("p1", "p2")));
            assertEquals(4L, cost);
        }

        {   // Two role-players and relation
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ $r ($p1, $p2) isa household; }", transaction.logic()));
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("p1", "p2")));
            assertEquals(4L, cost);
        }
    }

    @Test
    public void test_inferred_explicit_has() {
        initialise(Arguments.Session.Type.SCHEMA, Arguments.Transaction.Type.WRITE);
        transaction.query().define(TypeQL.parseQuery("define " +
                "rule everyone-is-named-dave: " +
                "when { $x isa person; } " +
                "then { $x has first-name \"Dave\"; };"));
        transaction.commit();
        session.close();

        initialise(Arguments.Session.Type.DATA, Arguments.Transaction.Type.READ);
        CostEstimator costEstimator = new CostEstimator(transaction.logic());
        {   // The explicit has should add one to the estimate of $n
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ $p has name $n; }", transaction.logic()));
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("n")));
            assertEquals(6L, cost);
        }
    }

    @Test
    public void test_inferred_relation() {
        initialise(Arguments.Session.Type.SCHEMA, Arguments.Transaction.Type.WRITE);
        transaction.query().define(TypeQL.parseQuery("define " +
                "symmetric-friendship sub relation, relates friendor, relates friendee;" +
                "person plays symmetric-friendship:friendor, plays symmetric-friendship:friendee;" +
                "rule friendship-is-symmetric-1: " +
                "when { (friendor: $f1, friendee: $f2) isa friendship; } " +
                "then { (friendor: $f1, friendee: $f2) isa symmetric-friendship; };" +
                "rule friendship-is-symmetric-2: " +
                "when { (friendor: $f1, friendee: $f2) isa friendship; } " +
                "then { (friendor: $f2, friendee: $f1) isa symmetric-friendship; };"));
        transaction.query().define(TypeQL.parseQuery("define " +
                "jealous sub relation, relates who, relates whom;" +
                "person plays jealous:who, plays jealous:whom;" +
                "rule everyone-is-jealous-of-everyone: " +
                "when { $p1 isa person; $p2 isa person; } " +
                "then { (who: $p1, whom: $p2) isa jealous; };"));

        transaction.commit();
        session.close();

        initialise(Arguments.Session.Type.DATA, Arguments.Transaction.Type.READ);
        CostEstimator costEstimator = new CostEstimator(transaction.logic());
        {
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ $f isa symmetric-friendship; }", transaction.logic()));
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("f")));
            assertEquals(6L, cost);
        }

        {
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ $j isa jealous; }", transaction.logic()));
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("j")));
            assertEquals(25L, cost);
        }

        {
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ (who:$x, whom:$y) isa jealous; }", transaction.logic()));
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("x", "y")));
            assertEquals(25L, cost);
        }

        {
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ (who:$x, whom:$y) isa jealous; }", transaction.logic()));
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("x")));
            assertEquals(5L, cost);
        }

        {   // Is not inferred.
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ " +
                    "(friendor: $x, friendee: $y) isa friendship;" +
                    "(friendor: $y, friendee: $z) isa friendship; " +
                    "}", transaction.logic()));
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("x", "z")));
            assertEquals(9L, cost); // TODO: 9 because it's currently at local, disconnected estimates.
        }

        {   // Will work with only the relation-roleplayer and no sibling roleplayer co-constraints
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ " +
                    "(friendor: $x, friendee: $y) isa symmetric-friendship;" +
                    "(friendor: $y, friendee: $z) isa symmetric-friendship; " +
                    "}", transaction.logic()));
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("x", "z")));
            assertEquals(25L, cost); // TODO: 25 because it's currently at local, disconnected estimates.
        }

        {   // Requires sibling roleplayer co-constraints
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ " +
                    "(friendor: $x, friendee: $y) isa symmetric-friendship;" +
                    "(friendor: $y, friendee: $z) isa symmetric-friendship; " +
                    "}", transaction.logic()));
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("x", "y", "z")));
            assertEquals(36L, cost); // {x} (or {x,y}) and {y,z} (or {z}) is each covered by the estimate for symmetric-friendship -> 6 * 6 = 36
        }
    }

    @Test
    public void test_inferred_relation_projection() {
        initialise(Arguments.Session.Type.SCHEMA, Arguments.Transaction.Type.WRITE);
        transaction.query().define(TypeQL.parseQuery("define " +
                "one-hop-friends sub relation, relates first, relates second, relates third;" +
                "person plays one-hop-friends:first, plays one-hop-friends:second, plays one-hop-friends:third;" +
                "rule all-ternary: " +
                "when { (friendor: $p1, friendee: $p2) isa friendship; (friendor: $p2, friendee: $p3) isa friendship; } " +
                "then { (first: $p1, second: $p2, third: $p3) isa one-hop-friends; };"));

        transaction.commit();
        session.close();

        initialise(Arguments.Session.Type.DATA, Arguments.Transaction.Type.READ);
        CostEstimator costEstimator = new CostEstimator(transaction.logic());
        {
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{(first: $p1, second: $p2, third: $p3) isa one-hop-friends; }", transaction.logic()));
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("p1", "p2", "p3")));
            assertEquals(9, cost);
        }

        {   // Works without fancy projection
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ (first: $p1) isa one-hop-friends; }", transaction.logic()));
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("p1")));
            assertEquals(3, cost);
        }

        boolean FANCY_PROJECTION_IS_IMPLEMENTED = false;
        if (FANCY_PROJECTION_IS_IMPLEMENTED) {   // Needs fancy projection
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ (first: $p1, second: $p2, third: $p3) isa one-hop-friends; }", transaction.logic()));
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("p1")));
            assertEquals(3, cost);
        }
    }

    @Test
    public void test_included_resolvables() {
        initialise(Arguments.Session.Type.SCHEMA, Arguments.Transaction.Type.WRITE);
        transaction.query().define(TypeQL.parseQuery("define " +
                "symmetric-friendship sub relation, relates friendor, relates friendee;" +
                "person plays symmetric-friendship:friendor, plays symmetric-friendship:friendee;" +
                "rule friendship-is-symmetric-1: " +
                "when { (friendor: $f1, friendee: $f2) isa friendship; } " +
                "then { (friendor: $f1, friendee: $f2) isa symmetric-friendship; };" +
                "rule friendship-is-symmetric-2: " +
                "when { (friendor: $f1, friendee: $f2) isa friendship; } " +
                "then { (friendor: $f2, friendee: $f1) isa symmetric-friendship; };"));
        transaction.query().define(TypeQL.parseQuery("define " +
                "jealous sub relation, relates who, relates whom;" +
                "person plays jealous:who, plays jealous:whom;" +
                "rule everyone-is-jealous-of-everyone: " +
                "when { $p1 isa person; $p2 isa person; } " +
                "then { (who: $p1, whom: $p2) isa jealous; };"));

        transaction.commit();
        session.close();

        initialise(Arguments.Session.Type.DATA, Arguments.Transaction.Type.READ);
        CostEstimator costEstimator = new CostEstimator(transaction.logic());
        {   // Total answers
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ $f (friendor: $p1, friendee: $p2) isa friendship; $j (who: $p1, whom: $p2) isa jealous;  }", transaction.logic()));
            Set<Resolvable<?>> resolvables = transaction.logic().compile(conjunction);
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("p1", "p2")), resolvables);
            assertEquals(3L, cost);
        }

        {   // With just jealous
            ResolvableConjunction conjunction = ResolvableConjunction.of(resolvedConjunction("{ $f (friendor: $p1, friendee: $p2) isa friendship; $j (who: $p1, whom: $p2) isa jealous;  }", transaction.logic()));
            Set<Resolvable<?>> resolvables = transaction.logic().compile(conjunction);
            Set<Resolvable<?>> justJealous = resolvables.stream().filter(resolvable -> resolvable.isConcludable()).collect(Collectors.toSet());
            long cost = costEstimator.estimateAnswers(conjunction, getVariablesByName(conjunction.pattern(), set("p1", "p2")), justJealous);
            assertEquals(25L, cost);
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

    private Set<Variable> getVariablesByName(Conjunction conjunctionPattern, Set<String> names) {
        return names.stream()
                .map(name -> conjunctionPattern.variable(Identifier.Variable.Retrievable.of(Reference.Name.name(name))))
                .collect(Collectors.toSet());
    }
}
