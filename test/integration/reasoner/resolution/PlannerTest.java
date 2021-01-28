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
import grakn.core.concept.ConceptManager;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.logic.LogicManager;
import grakn.core.logic.resolvable.Concludable;
import grakn.core.logic.resolvable.Resolvable;
import grakn.core.logic.resolvable.Retrievable;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
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
import static junit.framework.TestCase.assertEquals;

public class PlannerTest {

    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("resolver-manager-test");
    private static String database = "resolver-manager-test";
    private static RocksGrakn grakn;
    private static RocksSession session;
    private static RocksTransaction rocksTransaction;
    private static ConceptManager conceptMgr;
    private static LogicManager logicMgr;

    @Before
    public void setUp() throws IOException {
        Util.resetDirectory(directory);
        grakn = RocksGrakn.open(directory);
        grakn.databases().create(database);
        newTransaction(Arguments.Session.Type.SCHEMA, Arguments.Transaction.Type.WRITE);
    }

    private void newTransaction(Arguments.Session.Type schema, Arguments.Transaction.Type write) {
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

    private Conjunction parse(String query) {
        return logicMgr.typeResolver().resolve(Disjunction.create(Graql.parsePattern(query).asConjunction().normalise())
                                                       .conjunctions().iterator().next());
    }

    @Test
    public void test_planner_retrievable_dependent_upon_concludable() {
        Concludable concludable = Concludable.create(parse("{ $a has $b; }")).iterator().next();
        Retrievable retrievable = new Retrievable(parse("{ $c($b); }"));

        Set<Resolvable> resolvables = set(concludable, retrievable);
        List<Resolvable> plan = new Planner(conceptMgr, logicMgr).plan(resolvables);
        assertEquals(list(concludable, retrievable), plan);
    }

    @Test
    public void test_planner_prioritises_retrievable_without_dependencies() {
        EntityType person = conceptMgr.putEntityType("person");
        person.setOwns(conceptMgr.putAttributeType("name", AttributeType.ValueType.STRING));

        Concludable concludable = Concludable.create(parse("{ $p has name $n; }")).iterator().next();
        Retrievable retrievable = new Retrievable(parse("{ $p isa person; }"));

        Set<Resolvable> resolvables = set(concludable, retrievable);

        List<Resolvable> plan = new Planner(conceptMgr, logicMgr).plan(resolvables);
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

        Retrievable retrievable = new Retrievable(parse("{ $p isa person, has age $a, has first-name $fn, has " +
                                                                "surname $sn; }"));
        Concludable concludable = Concludable.create(parse("{ ($p, $c); }")).iterator().next();
        Retrievable retrievable2 = new Retrievable(parse("{ $c isa company, has name $cn; }"));

        Set<Resolvable> resolvables = set(retrievable, retrievable2, concludable);

        List<Resolvable> plan = new Planner(conceptMgr, logicMgr).plan(resolvables);
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

        Retrievable retrievable = new Retrievable(parse("{ $p isa person, has age 30, has first-name " +
                                                                "\"Alice\", has surname \"Bachelor\"; }"));
        Concludable concludable = Concludable.create(parse("{ ($p, $c); }")).iterator().next();
        Retrievable retrievable2 = new Retrievable(parse("{ $c isa company, has name $cn; }"));

        Set<Resolvable> resolvables = set(retrievable, retrievable2, concludable);

        List<Resolvable> plan = new Planner(conceptMgr, logicMgr).plan(resolvables);
        assertEquals(list(retrievable2, concludable, retrievable), plan);
    }

    @Test
    public void test_planner_starts_at_independent_concludable() {
        Concludable concludable = Concludable.create(parse("{ $r($a, $b); }")).iterator().next();
        Concludable concludable2 = Concludable.create(parse("{ $r has $c; }")).iterator().next();

        Set<Resolvable> resolvables = set(concludable, concludable2);

        List<Resolvable> plan = new Planner(conceptMgr, logicMgr).plan(resolvables);
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

        Retrievable retrievable = new Retrievable(parse("{ $p isa person; }"));
        Concludable concludable = Concludable.create(parse("{ $p has name $n; }")).iterator().next();
        Retrievable retrievable2 = new Retrievable(parse("{ $c isa company, has name $n; }"));
        Concludable concludable2 = Concludable.create(parse("{ $e($c, $p2) isa employment; }")).iterator().next();

        Set<Resolvable> resolvables = set(retrievable, retrievable2, concludable, concludable2);
        List<Resolvable> plan = new Planner(conceptMgr, logicMgr).plan(resolvables);

        assertEquals(list(retrievable, concludable, retrievable2, concludable2), plan);
    }

    @Test
    public void test_planner_two_circular_has_dependencies() {
        Concludable concludable = Concludable.create(parse("{ $a has $b; }")).iterator().next();
        Concludable concludable2 = Concludable.create(parse("{ $b has $a; }")).iterator().next();

        Set<Resolvable> resolvables = set(concludable, concludable2);
        List<Resolvable> plan = new Planner(conceptMgr, logicMgr).plan(resolvables);

        assertEquals(2, plan.size());
        assertEquals(set(concludable, concludable2), set(plan));
    }

    @Test
    public void test_planner_two_circular_relates_dependencies() {
        Concludable concludable = Concludable.create(parse("{ $a($b); }")).iterator().next();
        Concludable concludable2 = Concludable.create(parse("{ $b($a); }")).iterator().next();

        Set<Resolvable> resolvables = set(concludable, concludable2);
        List<Resolvable> plan = new Planner(conceptMgr, logicMgr).plan(resolvables);

        assertEquals(2, plan.size());
        assertEquals(set(concludable, concludable2), set(plan));
    }

    @Test
    public void test_planner_disconnected_conjunction() {
        Concludable concludable = Concludable.create(parse("{ $a($b); }")).iterator().next();
        Concludable concludable2 = Concludable.create(parse("{ $c($d); }")).iterator().next();

        Set<Resolvable> resolvables = set(concludable, concludable2);
        List<Resolvable> plan = new Planner(conceptMgr, logicMgr).plan(resolvables);

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
        logicMgr.putRule(
                "marriage-is-friendship",
                Graql.parsePattern("{$x isa person; $y isa person; (spouse: $x, spouse: $y) isa marriage; }").asConjunction(),
                Graql.parseVariable("(friend: $x, friend: $y) isa friendship").asThing());
        rocksTransaction.commit();
        session.close();
        newTransaction(Arguments.Session.Type.DATA, Arguments.Transaction.Type.READ);

        Concludable concludable = Concludable.create(parse("{ $b has $a; }")).iterator().next();
        Concludable concludable2 = Concludable.create(parse("{ $c($b) isa friendship; }")).iterator().next();

        Set<Resolvable> resolvables = set(concludable, concludable2);
        List<Resolvable> plan = new Planner(conceptMgr, logicMgr).plan(resolvables);

        assertEquals(0, concludable.getApplicableRules(conceptMgr, logicMgr).toList().size());
        assertEquals(1, concludable2.getApplicableRules(conceptMgr, logicMgr).toList().size());
        assertEquals(list(concludable, concludable2), plan);
    }
}
