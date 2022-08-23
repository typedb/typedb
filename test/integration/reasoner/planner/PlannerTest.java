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
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typedb.core.database.CoreSession;
import com.vaticle.typedb.core.database.CoreTransaction;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.TypeQL;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static junit.framework.TestCase.assertEquals;

public class PlannerTest {

    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("resolver-manager-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Database options = new Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
            .storageDataCacheSize(MB).storageIndexCacheSize(MB);
    private static final String database = "resolver-manager-test";
    private static CoreDatabaseManager databaseMgr;
    private static CoreSession session;
    private static CoreTransaction transaction;
    private static ConceptManager conceptMgr;
    private static LogicManager logicMgr;

    private static class ReasonerPlannerTestWrapper extends GreedyCostSearch.OldPlannerEmulator {

        private final Set<Resolvable<?>> mockResolvables;
        public ResolvableConjunction mockConjunction;

        public ReasonerPlannerTestWrapper(CoreTransaction tx, Set<Resolvable<?>> mockResolvables) {
            super(tx.traversal(), tx.concepts(), tx.logic());
            this.mockResolvables = mockResolvables;
            this.mockConjunction = ResolvableConjunction.of(new Conjunction(set(), new ArrayList<>()));
        }

        @Override
        public Set<Resolvable<?>> compile(ResolvableConjunction conjunction){
            return mockResolvables;
        }

        private static List<Resolvable<?>> planResolvables(Set<Resolvable<?>> resolvables, Set<Identifier.Variable.Retrievable> inputBounds) {
            ReasonerPlannerTestWrapper planSearch = new ReasonerPlannerTestWrapper(transaction, resolvables);
            return planSearch.planConjunction(planSearch.mockConjunction, inputBounds).planOrder();
        }
    }

    @Before
    public void setUp() throws IOException {
        Util.resetDirectory(dataDir);
        databaseMgr = CoreDatabaseManager.open(options);
        databaseMgr.create(database);
        initialise(Arguments.Session.Type.SCHEMA, Arguments.Transaction.Type.WRITE);
        transaction.query().define(TypeQL.parseQuery("define person sub entity, plays friendship:friend, owns name; " +
                "friendship sub relation, relates friend;" +
                "name sub attribute, value string;"));
        transaction.commit();
        session.close();
        initialise(Arguments.Session.Type.SCHEMA, Arguments.Transaction.Type.WRITE);
    }

    private void initialise(Arguments.Session.Type schema, Arguments.Transaction.Type write) {
        session = databaseMgr.session(database, schema);
        transaction = session.transaction(write);
        conceptMgr = transaction.concepts();
        logicMgr = transaction.logic();
    }

    @After
    public void tearDown() {
        transaction.close();
        session.close();
        databaseMgr.close();
    }

    @Test
    public void test_planner_retrievable_dependent_upon_concludable() {
        Concludable concludable = Concludable.create(resolvedConjunction("{ $a has $b; }", logicMgr)).iterator().next();
        Retrievable retrievable = new Retrievable(resolvedConjunction("{ $c($b); }", logicMgr));

        Set<Resolvable<?>> resolvables = set(concludable, retrievable);
        List<Resolvable<?>> plan = ReasonerPlannerTestWrapper.planResolvables(resolvables, set());
        assertEquals(list(concludable, retrievable), plan);
    }

    @Test
    public void test_planner_prioritises_retrievable_without_dependencies() {
        Concludable concludable = Concludable.create(resolvedConjunction("{ $p has name $n; }", logicMgr)).iterator().next();
        Retrievable retrievable = new Retrievable(resolvedConjunction("{ $p isa person; }", logicMgr));

        Set<Resolvable<?>> resolvables = set(concludable, retrievable);

        List<Resolvable<?>> plan = ReasonerPlannerTestWrapper.planResolvables(resolvables, set());
        assertEquals(list(retrievable, concludable), plan);
    }

    @Test
    public void test_planner_starts_at_independent_concludable() {
        Concludable concludable = Concludable.create(resolvedConjunction("{ $r($a, $b); }", logicMgr)).iterator().next();
        Concludable concludable2 = Concludable.create(resolvedConjunction("{ $r has $c; }", logicMgr)).iterator().next();

        Set<Resolvable<?>> resolvables = set(concludable, concludable2);

        List<Resolvable<?>> plan = ReasonerPlannerTestWrapper.planResolvables(resolvables, set());
        assertEquals(list(concludable, concludable2), plan);
    }

    @Test
    public void test_planner_multiple_dependencies() {
        transaction.query().define(TypeQL.parseQuery("define employment sub relation, relates employee, relates employer;" +
                "person plays employment:employee;" +
                "company sub entity, plays employment:employer, owns name;"));
        transaction.commit();
        session.close();
        initialise(Arguments.Session.Type.DATA, Arguments.Transaction.Type.READ);

        EntityType person = conceptMgr.putEntityType("person");
        AttributeType name = conceptMgr.putAttributeType("name", AttributeType.ValueType.STRING);
        person.setOwns(name);
        EntityType company = conceptMgr.putEntityType("company");
        company.setOwns(name);
        conceptMgr.putRelationType("employment");

        Retrievable retrievable = new Retrievable(resolvedConjunction("{ $p isa person; }", logicMgr));
        Concludable concludable = Concludable.create(resolvedConjunction("{ $p has name $n; }", logicMgr)).iterator().next();
        Retrievable retrievable2 = new Retrievable(resolvedConjunction("{ $c isa company, has name $n; }", logicMgr));
        Concludable concludable2 = Concludable.create(resolvedConjunction("{ $e($c, $p2) isa employment; }", logicMgr)).iterator().next();

        Set<Resolvable<?>> resolvables = set(retrievable, retrievable2, concludable, concludable2);
        List<Resolvable<?>> plan = ReasonerPlannerTestWrapper.planResolvables(resolvables, set());

        assertEquals(list(retrievable, concludable, retrievable2, concludable2), plan);
    }

    @Test
    public void test_planner_two_circular_has_dependencies() {
        Concludable concludable = Concludable.create(resolvedConjunction("{ $a has $b; }", logicMgr)).iterator().next();
        Concludable concludable2 = Concludable.create(resolvedConjunction("{ $b has $a; }", logicMgr)).iterator().next();

        Set<Resolvable<?>> resolvables = set(concludable, concludable2);
        List<Resolvable<?>> plan = ReasonerPlannerTestWrapper.planResolvables(resolvables, set());

        assertEquals(2, plan.size());
        assertEquals(set(concludable, concludable2), set(plan));
    }

    @Test
    public void test_planner_two_circular_relates_dependencies() {
        Concludable concludable = Concludable.create(resolvedConjunction("{ $a($b); }", logicMgr)).iterator().next();
        Concludable concludable2 = Concludable.create(resolvedConjunction("{ $b($a); }", logicMgr)).iterator().next();

        Set<Resolvable<?>> resolvables = set(concludable, concludable2);
        List<Resolvable<?>> plan = ReasonerPlannerTestWrapper.planResolvables(resolvables, set());

        assertEquals(2, plan.size());
        assertEquals(set(concludable, concludable2), set(plan));
    }

    @Test
    public void test_planner_disconnected_conjunction() {
        Concludable concludable = Concludable.create(resolvedConjunction("{ $a($b); }", logicMgr)).iterator().next();
        Concludable concludable2 = Concludable.create(resolvedConjunction("{ $c($d); }", logicMgr)).iterator().next();

        Set<Resolvable<?>> resolvables = set(concludable, concludable2);
        List<Resolvable<?>> plan = ReasonerPlannerTestWrapper.planResolvables(resolvables, set());

        assertEquals(2, plan.size());
        assertEquals(set(concludable, concludable2), set(plan));
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
