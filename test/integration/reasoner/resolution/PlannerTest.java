/*
 * Copyright (C) 2021 Grakn Labs
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
 */

package grakn.core.reasoner.resolution;

import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Options.Database;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.logic.LogicManager;
import grakn.core.logic.resolvable.Concludable;
import grakn.core.logic.resolvable.Resolvable;
import grakn.core.logic.resolvable.Retrievable;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import grakn.core.test.integration.util.Util;
import graql.lang.Graql;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.set;
import static grakn.core.reasoner.resolution.Util.resolvedConjunction;
import static junit.framework.TestCase.assertEquals;

public class PlannerTest {

    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("resolver-manager-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Database options = new Database().dataDir(dataDir).logsDir(logDir);
    private static final String database = "resolver-manager-test";
    private static RocksGrakn grakn;
    private static RocksSession session;
    private static RocksTransaction rocksTransaction;
    private static ConceptManager conceptMgr;
    private static LogicManager logicMgr;

    @Before
    public void setUp() throws IOException {
        Util.resetDirectory(dataDir);
        grakn = RocksGrakn.open(options);
        grakn.databases().create(database);
        initialise(Arguments.Session.Type.SCHEMA, Arguments.Transaction.Type.WRITE);
    }

    private void initialise(Arguments.Session.Type schema, Arguments.Transaction.Type write) {
        session = grakn.session(database, schema);
        rocksTransaction = session.transaction(write);
        conceptMgr = rocksTransaction.concepts();
        logicMgr = rocksTransaction.logic();
    }

    @After
    public void tearDown() {
        rocksTransaction.close();
        session.close();
        grakn.close();
    }

    private void defineBasicSchema() {
        rocksTransaction.query().define(Graql.parseQuery("define person sub entity, plays friendship:friend, owns name; " +
                                                                 "friendship sub relation, relates friend;" +
                                                                 "name sub attribute, value string;"));
        rocksTransaction.commit();
        session.close();
        initialise(Arguments.Session.Type.SCHEMA, Arguments.Transaction.Type.WRITE);
    }

    @Test
    public void test_planner_retrievable_dependent_upon_concludable() {
        defineBasicSchema();
        Concludable concludable = Concludable.create(resolvedConjunction("{ $a has $b; }", logicMgr)).iterator().next();
        Retrievable retrievable = new Retrievable(resolvedConjunction("{ $c($b); }", logicMgr));

        Set<Resolvable<?>> resolvables = set(concludable, retrievable);
        List<Resolvable<?>> plan = new Planner(conceptMgr, logicMgr).plan(resolvables, set());
        assertEquals(list(concludable, retrievable), plan);
    }

    @Test
    public void test_planner_prioritises_retrievable_without_dependencies() {
        defineBasicSchema();
        Concludable concludable = Concludable.create(resolvedConjunction("{ $p has name $n; }", logicMgr)).iterator().next();
        Retrievable retrievable = new Retrievable(resolvedConjunction("{ $p isa person; }", logicMgr));

        Set<Resolvable<?>> resolvables = set(concludable, retrievable);

        List<Resolvable<?>> plan = new Planner(conceptMgr, logicMgr).plan(resolvables, set());
        assertEquals(list(retrievable, concludable), plan);
    }

    @Test
    public void test_planner_prioritises_largest_retrievable_without_dependencies() {
        EntityType person = conceptMgr.putEntityType("person");
        person.setOwns(conceptMgr.putAttributeType("first-name", AttributeType.ValueType.STRING));
        person.setOwns(conceptMgr.putAttributeType("surname", AttributeType.ValueType.STRING));
        person.setOwns(conceptMgr.putAttributeType("age", AttributeType.ValueType.STRING));
        EntityType company = conceptMgr.putEntityType("company");
        company.setOwns(conceptMgr.putAttributeType("name", AttributeType.ValueType.STRING));

        Retrievable retrievable = new Retrievable(resolvedConjunction("{ $p isa person, has age $a, has first-name $fn, has " +
                                                                "surname $sn; }", logicMgr));
        Concludable concludable = Concludable.create(resolvedConjunction("{ ($p, $c); }", logicMgr)).iterator().next();
        Retrievable retrievable2 = new Retrievable(resolvedConjunction("{ $c isa company, has name $cn; }", logicMgr));

        Set<Resolvable<?>> resolvables = set(retrievable, retrievable2, concludable);

        List<Resolvable<?>> plan = new Planner(conceptMgr, logicMgr).plan(resolvables, set());
        assertEquals(list(retrievable, concludable, retrievable2), plan);
    }

    @Test
    public void test_planner_prioritises_largest_named_variables_retrievable_without_dependencies() {
        EntityType person = conceptMgr.putEntityType("person");
        person.setOwns(conceptMgr.putAttributeType("first-name", AttributeType.ValueType.STRING));
        person.setOwns(conceptMgr.putAttributeType("surname", AttributeType.ValueType.STRING));
        person.setOwns(conceptMgr.putAttributeType("age", AttributeType.ValueType.STRING));
        EntityType company = conceptMgr.putEntityType("company");
        company.setOwns(conceptMgr.putAttributeType("name", AttributeType.ValueType.STRING));

        Retrievable retrievable = new Retrievable(resolvedConjunction("{ $p isa person, has age 30, has first-name " +
                                                                "\"Alice\", has surname \"Bachelor\"; }", logicMgr));
        Concludable concludable = Concludable.create(resolvedConjunction("{ ($p, $c); }", logicMgr)).iterator().next();
        Retrievable retrievable2 = new Retrievable(resolvedConjunction("{ $c isa company, has name $cn; }", logicMgr));

        Set<Resolvable<?>> resolvables = set(retrievable, retrievable2, concludable);

        List<Resolvable<?>> plan = new Planner(conceptMgr, logicMgr).plan(resolvables, set());
        assertEquals(list(retrievable2, concludable, retrievable), plan);
    }

    @Test
    public void test_planner_starts_at_independent_concludable() {
        defineBasicSchema();
        Concludable concludable = Concludable.create(resolvedConjunction("{ $r($a, $b); }", logicMgr)).iterator().next();
        Concludable concludable2 = Concludable.create(resolvedConjunction("{ $r has $c; }", logicMgr)).iterator().next();

        Set<Resolvable<?>> resolvables = set(concludable, concludable2);

        List<Resolvable<?>> plan = new Planner(conceptMgr, logicMgr).plan(resolvables, set());
        assertEquals(list(concludable, concludable2), plan);
    }

    @Test
    public void test_planner_multiple_dependencies() {
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
        List<Resolvable<?>> plan = new Planner(conceptMgr, logicMgr).plan(resolvables, set());

        assertEquals(list(retrievable, concludable, retrievable2, concludable2), plan);
    }

    @Test
    public void test_planner_two_circular_has_dependencies() {
        defineBasicSchema();
        Concludable concludable = Concludable.create(resolvedConjunction("{ $a has $b; }", logicMgr)).iterator().next();
        Concludable concludable2 = Concludable.create(resolvedConjunction("{ $b has $a; }", logicMgr)).iterator().next();

        Set<Resolvable<?>> resolvables = set(concludable, concludable2);
        List<Resolvable<?>> plan = new Planner(conceptMgr, logicMgr).plan(resolvables, set());

        assertEquals(2, plan.size());
        assertEquals(set(concludable, concludable2), set(plan));
    }

    @Test
    public void test_planner_two_circular_relates_dependencies() {
        defineBasicSchema();
        Concludable concludable = Concludable.create(resolvedConjunction("{ $a($b); }", logicMgr)).iterator().next();
        Concludable concludable2 = Concludable.create(resolvedConjunction("{ $b($a); }", logicMgr)).iterator().next();

        Set<Resolvable<?>> resolvables = set(concludable, concludable2);
        List<Resolvable<?>> plan = new Planner(conceptMgr, logicMgr).plan(resolvables, set());

        assertEquals(2, plan.size());
        assertEquals(set(concludable, concludable2), set(plan));
    }

    @Test
    public void test_planner_disconnected_conjunction() {
        defineBasicSchema();
        Concludable concludable = Concludable.create(resolvedConjunction("{ $a($b); }", logicMgr)).iterator().next();
        Concludable concludable2 = Concludable.create(resolvedConjunction("{ $c($d); }", logicMgr)).iterator().next();

        Set<Resolvable<?>> resolvables = set(concludable, concludable2);
        List<Resolvable<?>> plan = new Planner(conceptMgr, logicMgr).plan(resolvables, set());

        assertEquals(2, plan.size());
        assertEquals(set(concludable, concludable2), set(plan));
    }

    @Test
    public void test_planner_prioritises_concludable_with_least_applicable_rules() {
        EntityType person = conceptMgr.putEntityType("person");
        RelationType friendship = conceptMgr.putRelationType("friendship");
        friendship.setRelates("friend");
        RelationType marriage = conceptMgr.putRelationType("marriage");
        marriage.setRelates("spouse");
        person.setPlays(friendship.getRelates("friend"));
        person.setPlays(marriage.getRelates("spouse"));
        AttributeType name = conceptMgr.putAttributeType("name", AttributeType.ValueType.STRING);
        person.setOwns(name);
        logicMgr.putRule(
                "marriage-is-friendship",
                Graql.parsePattern("{$x isa person; $y isa person; (spouse: $x, spouse: $y) isa marriage; }").asConjunction(),
                Graql.parseVariable("(friend: $x, friend: $y) isa friendship").asThing());
        rocksTransaction.commit();
        session.close();
        initialise(Arguments.Session.Type.DATA, Arguments.Transaction.Type.READ);

        Concludable concludable = Concludable.create(resolvedConjunction("{ $b has $a; }", logicMgr)).iterator().next();
        Concludable concludable2 = Concludable.create(resolvedConjunction("{ $c($b) isa friendship; }", logicMgr)).iterator().next();

        Set<Resolvable<?>> resolvables = set(concludable, concludable2);
        List<Resolvable<?>> plan = new Planner(conceptMgr, logicMgr).plan(resolvables, set());

        assertEquals(0, concludable.getApplicableRules(conceptMgr, logicMgr).toList().size());
        assertEquals(1, concludable2.getApplicableRules(conceptMgr, logicMgr).toList().size());
        assertEquals(list(concludable, concludable2), plan);
    }
}
