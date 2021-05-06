/*
 * Copyright (C) 2021 Vaticle
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.vaticle.typedb.core.logic.resolvable;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Relation;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.rocks.RocksSession;
import com.vaticle.typedb.core.rocks.RocksTransaction;
import com.vaticle.typedb.core.rocks.RocksTypeDB;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.TypeQL;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.map;
import static com.vaticle.typedb.common.collection.Collections.pair;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.logic.resolvable.Util.createRule;
import static com.vaticle.typedb.core.logic.resolvable.Util.getStringMapping;
import static com.vaticle.typedb.core.logic.resolvable.Util.resolvedConjunction;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UnifyRelationConcludableTest {

    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("unify-relation-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).logsDir(logDir);
    private static final String database = "unify-relation-test";
    private static RocksTypeDB typedb;
    private static RocksSession session;
    private static RocksTransaction rocksTransaction;
    private static ConceptManager conceptMgr;
    private static LogicManager logicMgr;

    @BeforeClass
    public static void setUp() throws IOException {
        Util.resetDirectory(dataDir);
        typedb = RocksTypeDB.open(options);
        typedb.databases().create(database);
        session = typedb.session(database, Arguments.Session.Type.SCHEMA);
        try (RocksTransaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
            tx.query().define(TypeQL.parseQuery("define " +
                                                        "person sub entity," +
                                                        "    owns first-name," +
                                                        "    owns last-name," +
                                                        "    owns age," +
                                                        "    plays employment:employee," +
                                                        "    plays employment:employer," +
                                                        "    plays part-time-employment:part-time-employee," +
                                                        "    plays friendship:friend;" +
                                                        "company sub entity," +
                                                        "    plays employment:employer;" +
                                                        "employment sub relation," +
                                                        "    relates employee," +
                                                        "    relates employer;" +
                                                        "part-time-employment sub employment," +
                                                        "    relates part-time-employee as employee," +
                                                        "    relates restriction;" +
                                                        "restricted-entity sub entity, " +
                                                        "    plays part-time-employment:restriction;" +
                                                        "friendship sub relation," +
                                                        "    relates friend;" +
                                                        "name sub attribute, value string, abstract;" +
                                                        "first-name sub name;" +
                                                        "last-name sub name;" +
                                                        "age sub attribute, value long;" +
                                                        "").asDefine());
            tx.commit();
        }
    }

    @AfterClass
    public static void tearDown() {
        session.close();
        typedb.close();
    }

    @Before
    public void setUpTransaction() {
        rocksTransaction = session.transaction(Arguments.Transaction.Type.WRITE);
        conceptMgr = rocksTransaction.concepts();
        logicMgr = rocksTransaction.logic();
    }

    @After
    public void tearDownTransaction() {
        rocksTransaction.close();
    }

    private Thing instanceOf(String label) {
        ThingType type = conceptMgr.getThingType(label);
        assert type != null : "Cannot find type " + label;
        if (type.isEntityType()) return type.asEntityType().create();
        else if (type.isRelationType()) return type.asRelationType().create();
        else if (type.isAttributeType() && type.asAttributeType().isString())
            return type.asAttributeType().asString().put("john");
        else if (type.isAttributeType() && type.asAttributeType().isLong())
            return type.asAttributeType().asLong().put(10L);
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    private void addRolePlayer(Relation relation, String role, Thing player) {
        RelationType relationType = relation.getType();
        RoleType roleType = relationType.getRelates(role);
        assert roleType != null : "Role type " + role + " does not exist in relation type " + relation.getType().getLabel();
        relation.addPlayer(roleType, player);
    }

    @Test
    public void relation_and_player_unifies_rule_relation_exact() {
        String conjunction = "{ $r (employee: $y) isa employment; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Relation queryConcludable = concludables.iterator().next().asRelation();

        Rule rule = createRule("people-are-employed", "{ $x isa person; }",
                               " (employee: $x) isa employment ", logicMgr);

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion(), conceptMgr).toList();
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$y", set("$x"));
            put("$r", set("$_0"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(1, unifier.requirements().roleTypes().size());
        assertEquals(set(Label.of("employee", "employment"), Label.of("part-time-employee", "part-time-employment")),
                     unifier.requirements().roleTypes().get(Identifier.Variable.label("employment:employee")));
        assertEquals(1, unifier.requirements().isaExplicit().size());
        assertEquals(set(Label.of("employment"), Label.of("part-time-employment")),
                     unifier.requirements().isaExplicit().get(Identifier.Variable.name("r")));
        assertEquals(0, unifier.requirements().predicates().size());

        // test forward unification can reject an invalid partial answer
        ConceptMap unUnified = new ConceptMap(map(pair(Identifier.Variable.name("r"), instanceOf("friendship")),
                                                  pair(Identifier.Variable.name("y"), instanceOf("person"))));
        assertFalse(unifier.unify(unUnified).isPresent());

        // test filter allows a valid answer
        Relation employment = instanceOf("employment").asRelation();
        Thing person = instanceOf("person");
        addRolePlayer(employment, "employee", person);
        Map<Identifier.Variable, Concept> concepts = map(
                pair(Identifier.Variable.anon(0), employment),
                pair(Identifier.Variable.name("x"), person),
                pair(Identifier.Variable.label("employment"), employment.getType()),
                pair(Identifier.Variable.label("employment:employee"), employment.getType().getRelates("employee"))
        );
        Optional<ConceptMap> unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertTrue(unified.isPresent());
        assertEquals(2, unified.get().concepts().size());
        assertEquals(employment, unified.get().get("r"));
        assertEquals(person, unified.get().get("y"));

        // filter out invalid types
        Relation friendship = instanceOf("friendship").asRelation();
        person = instanceOf("person");
        addRolePlayer(friendship, "friend", person);
        concepts = map(
                pair(Identifier.Variable.anon(0), friendship),
                pair(Identifier.Variable.name("x"), person),
                pair(Identifier.Variable.label("employment"), friendship.getType()),
                pair(Identifier.Variable.label("employment:employee"), friendship.getType().getRelates("friend"))
        );
        unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertFalse(unified.isPresent());
    }

    @Test
    public void relation_type_and_player_unifies_rule_relation_exact() {
        String conjunction = "{ (employee: $y) isa $rel; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Relation queryConcludable = concludables.iterator().next().asRelation();

        Rule rule = createRule("people-are-employed", "{ $x isa person; }",
                               " (employee: $x) isa employment ", logicMgr);

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion(), conceptMgr).toList();
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$y", set("$x"));
            put("$rel", set("$_employment"));
            put("$_0", set("$_0"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(1, unifier.requirements().roleTypes().size());
        assertEquals(set(Label.of("employee", "employment"), Label.of("part-time-employee", "part-time-employment")),
                     unifier.requirements().roleTypes().get(Identifier.Variable.label("relation:employee")));
        assertEquals(0, unifier.requirements().isaExplicit().size());
        assertEquals(0, unifier.requirements().predicates().size());

        // test filter allows a valid answer
        Relation employment = instanceOf("employment").asRelation();
        Thing person = instanceOf("person");
        addRolePlayer(employment, "employee", person);
        Map<Identifier.Variable, Concept> concepts = map(
                pair(Identifier.Variable.anon(0), employment),
                pair(Identifier.Variable.name("x"), person),
                pair(Identifier.Variable.label("employment"), employment.getType()),
                pair(Identifier.Variable.label("employment:employee"), employment.getType().getRelates("employee"))
        );
        Optional<ConceptMap> unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertTrue(unified.isPresent());
        assertEquals(3, unified.get().concepts().size());
        assertEquals(employment.getType(), unified.get().get("rel"));
        assertEquals(person, unified.get().get("y"));
        assertEquals(employment, unified.get().get(Identifier.Variable.anon(0)));

        // filter out invalid types
        Relation friendship = instanceOf("friendship").asRelation();
        person = instanceOf("person");
        addRolePlayer(friendship, "friend", person);
        concepts = map(
                pair(Identifier.Variable.anon(0), friendship),
                pair(Identifier.Variable.name("x"), person),
                pair(Identifier.Variable.label("employment"), friendship.getType()),
                pair(Identifier.Variable.label("employment:employee"), friendship.getType().getRelates("friend"))
        );
        unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertFalse(unified.isPresent());
    }

    @Test
    public void relation_role_unifies_rule_relation_exact() {
        String conjunction = "{ ($role: $y) isa employment; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Relation queryConcludable = concludables.iterator().next().asRelation();

        Rule rule = createRule("people-are-employed", "{ $x isa person; }",
                               " (employee: $x) isa employment ", logicMgr);

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion(), conceptMgr).toList();
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$y", set("$x"));
            put("$role", set("$_employment:employee"));
            put("$_0", set("$_0"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(0, unifier.requirements().roleTypes().size());
        assertEquals(1, unifier.requirements().isaExplicit().size());
        assertEquals(set(Label.of("employment"), Label.of("part-time-employment")),
                     unifier.requirements().isaExplicit().get(Identifier.Variable.anon(0)));
        assertEquals(0, unifier.requirements().predicates().size());

        // test filter allows a valid answer
        Relation employment = instanceOf("employment").asRelation();
        Thing person = instanceOf("person");
        addRolePlayer(employment, "employee", person);
        Map<Identifier.Variable, Concept> concepts = map(
                pair(Identifier.Variable.anon(0), employment),
                pair(Identifier.Variable.name("x"), person),
                pair(Identifier.Variable.label("employment"), employment.getType()),
                pair(Identifier.Variable.label("employment:employee"), employment.getType().getRelates("employee"))
        );
        Optional<ConceptMap> unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertTrue(unified.isPresent());
        assertEquals(3, unified.get().concepts().size());
        assertEquals(employment.getType().getRelates("employee"), unified.get().get("role"));
        assertEquals(person, unified.get().get("y"));
        assertEquals(employment, unified.get().get(Identifier.Variable.anon(0)));

        // filter out invalid types
        Relation friendship = instanceOf("friendship").asRelation();
        person = instanceOf("person");
        addRolePlayer(friendship, "friend", person);
        concepts = map(
                pair(Identifier.Variable.anon(0), friendship),
                pair(Identifier.Variable.name("x"), person),
                pair(Identifier.Variable.label("employment"), friendship.getType()),
                pair(Identifier.Variable.label("employment:employee"), friendship.getType().getRelates("friend"))
        );
        unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertFalse(unified.isPresent());
    }

    @Test
    public void relation_without_isa_unifies_rule_relation() {
        String conjunction = "{ (employee: $y); }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Relation queryConcludable = concludables.iterator().next().asRelation();

        Rule rule = createRule("people-are-employed", "{ $x isa person; }",
                               " (employee: $x) isa employment ", logicMgr);

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion(), conceptMgr).toList();
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$y", set("$x"));
            put("$_0", set("$_0"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(1, unifier.requirements().roleTypes().size());
        assertEquals(set(Label.of("employee", "employment"), Label.of("part-time-employee", "part-time-employment")),
                     unifier.requirements().roleTypes().get(Identifier.Variable.label("relation:employee")));
        assertEquals(0, unifier.requirements().isaExplicit().size());
        assertEquals(0, unifier.requirements().predicates().size());
    }

    @Test
    public void relation_variables_one_to_many_unifiers() {
        String conjunction = "{ ($role: $p) isa employment; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Relation queryConcludable = concludables.iterator().next().asRelation();

        Rule rule = createRule("three-people-are-employed",
                               "{ $x isa person; $y isa person; $z isa person; }",
                               "(employee: $x, employee: $y, employee: $z) isa employment", logicMgr);

        FunctionalIterator<Unifier> unifier = queryConcludable.unify(rule.conclusion(), conceptMgr);
        Set<Map<String, Set<String>>> result = unifier.map(u -> getStringMapping(u.mapping())).toSet();

        Set<Map<String, Set<String>>> expected = set(
                new HashMap<String, Set<String>>() {{
                    put("$p", set("$x"));
                    put("$role", set("$_employment:employee"));
                    put("$_0", set("$_0"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$p", set("$y"));
                    put("$role", set("$_employment:employee"));
                    put("$_0", set("$_0"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$p", set("$z"));
                    put("$role", set("$_employment:employee"));
                    put("$_0", set("$_0"));
                }}
        );
        assertEquals(expected, result);
    }

    @Test
    public void relation_variable_multiple_identical_unifiers() {
        String conjunction = "{ (employee: $p) isa employment; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Relation queryConcludable = concludables.iterator().next().asRelation();

        Rule rule = createRule("the-same-person-is-employed-twice",
                               "{ $x isa person; $y isa person; $employment type employment; $employee type employment:employee; }",
                               "($employee: $x, $employee: $x) isa $employment", logicMgr);

        FunctionalIterator<Unifier> unifier = queryConcludable.unify(rule.conclusion(), conceptMgr);
        Set<Map<String, Set<String>>> result = unifier.map(u -> getStringMapping(u.mapping())).toSet();

        Set<Map<String, Set<String>>> expected = set(
                new HashMap<String, Set<String>>() {{
                    put("$p", set("$x"));
                    put("$_0", set("$_0"));
                }}
        );
        assertEquals(expected, result);
    }

    @Test
    public void unify_relation_many_to_many() {
        String conjunction = "{ (employee: $p, employee: $q) isa employment; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Relation queryConcludable = concludables.iterator().next().asRelation();

        Rule rule = createRule("three-people-are-employed",
                               "{ $x isa person; $y isa person; $z isa person; }",
                               "(employee: $x, employee: $y, employee: $z) isa employment", logicMgr);

        FunctionalIterator<Unifier> unifier = queryConcludable.unify(rule.conclusion(), conceptMgr);
        Set<Map<String, Set<String>>> result = unifier.map(u -> getStringMapping(u.mapping())).toSet();

        Set<Map<String, Set<String>>> expected = set(
                new HashMap<String, Set<String>>() {{
                    put("$p", set("$x"));
                    put("$q", set("$y"));
                    put("$_0", set("$_0"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$p", set("$x"));
                    put("$q", set("$z"));
                    put("$_0", set("$_0"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$p", set("$y"));
                    put("$q", set("$x"));
                    put("$_0", set("$_0"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$p", set("$y"));
                    put("$q", set("$z"));
                    put("$_0", set("$_0"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$p", set("$z"));
                    put("$q", set("$x"));
                    put("$_0", set("$_0"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$p", set("$z"));
                    put("$q", set("$y"));
                    put("$_0", set("$_0"));
                }}
        );
        assertEquals(expected, result);
    }

    @Test
    public void relation_player_role_unifies_rule_relation_repeated_variable_role() {
        String conjunction = "{ ($role: $p) isa employment; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Relation queryConcludable = concludables.iterator().next().asRelation();

        Rule rule = createRule("two-people-are-employed",
                               "{ $x isa person; $y isa person; $employment type employment; " +
                                       "$employee type employment:employee; $employer type employment:employer; }",
                               "($employee: $x, $employee: $y) isa $employment", logicMgr);

        FunctionalIterator<Unifier> unifier = queryConcludable.unify(rule.conclusion(), conceptMgr);
        Set<Map<String, Set<String>>> result = unifier.map(u -> getStringMapping(u.mapping())).toSet();

        Set<Map<String, Set<String>>> expected = set(
                new HashMap<String, Set<String>>() {{
                    put("$p", set("$x"));
                    put("$role", set("$employee"));
                    put("$_0", set("$_0"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$p", set("$y"));
                    put("$role", set("$employee"));
                    put("$_0", set("$_0"));
                }}
        );
        assertEquals(expected, result);
    }

    @Test
    public void relation_duplicate_players_unifies_rule_relation_distinct_players() {
        String conjunction = "{ (employee: $p, employee: $p) isa employment; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Relation queryConcludable = concludables.iterator().next().asRelation();

        Rule rule = createRule("two-people-are-employed",
                               "{ $x isa person; $y isa person; $employment type employment; " +
                                       "$employee type employment:employee; }",
                               "($employee: $x, $employee: $y) isa $employment", logicMgr);

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion(), conceptMgr).toList();
        Set<Map<String, Set<String>>> result = Iterators.iterate(unifiers).map(u -> getStringMapping(u.mapping())).toSet();

        Set<Map<String, Set<String>>> expected = set(
                new HashMap<String, Set<String>>() {{
                    put("$p", set("$x", "$y"));
                    put("$_0", set("$_0"));
                }}
        );
        assertEquals(expected, result);

        Unifier unifier = unifiers.get(0);

        // test requirements
        assertEquals(1, unifier.requirements().roleTypes().size());
        assertEquals(set(Label.of("employee", "employment"), Label.of("part-time-employee", "part-time-employment")),
                     unifier.requirements().roleTypes().get(Identifier.Variable.label("employment:employee")));
        assertEquals(1, unifier.requirements().isaExplicit().size());
        assertEquals(set(Label.of("employment"), Label.of("part-time-employment")),
                     unifier.requirements().isaExplicit().get(Identifier.Variable.anon(0)));
        assertEquals(0, unifier.requirements().predicates().size());

        // test filter allows a valid answer
        Relation employment = instanceOf("employment").asRelation();
        Thing person = instanceOf("person");
        addRolePlayer(employment, "employee", person);
        addRolePlayer(employment, "employee", person);
        Map<Identifier.Variable, Concept> concepts = map(
                pair(Identifier.Variable.anon(0), employment),
                pair(Identifier.Variable.name("x"), person),
                pair(Identifier.Variable.name("y"), person),
                pair(Identifier.Variable.name("employment"), employment.getType()),
                pair(Identifier.Variable.name("employee"), employment.getType().getRelates("employee"))
        );
        Optional<ConceptMap> unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertTrue(unified.isPresent());
        assertEquals(2, unified.get().concepts().size());
        assertEquals(person, unified.get().get("p"));
        assertEquals(employment, unified.get().get(Identifier.Variable.anon(0)));

        // filter out answers with differing role players that must be the same
        employment = instanceOf("employment").asRelation();
        person = instanceOf("person");
        Thing differentPerson = instanceOf("person");
        addRolePlayer(employment, "employee", person);
        addRolePlayer(employment, "employee", differentPerson);
        concepts = map(
                pair(Identifier.Variable.anon(0), employment),
                pair(Identifier.Variable.name("x"), person),
                pair(Identifier.Variable.name("y"), differentPerson),
                pair(Identifier.Variable.name("employment"), employment.getType()),
                pair(Identifier.Variable.name("employee"), employment.getType().getRelates("employee"))
        );
        unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertFalse(unified.isPresent());
    }

    @Test
    public void relation_unifies_many_to_many_rule_relation_players() {
        String conjunction = "{ (employee: $p, employer: $p, employee: $q) isa employment; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Relation queryConcludable = concludables.iterator().next().asRelation();

        Rule rule = createRule("two-people-are-employed-one-is-also-the-employer",
                               "{ $x isa person; $y isa person; }",
                               "(employee: $x, employer: $x, employee: $y) isa employment", logicMgr);

        List<Unifier> unifier = queryConcludable.unify(rule.conclusion(), conceptMgr).toList();
        List<Map<String, Set<String>>> result = Iterators.iterate(unifier).map(u -> getStringMapping(u.mapping())).toList();

        List<Map<String, Set<String>>> expected = list(
                new HashMap<String, Set<String>>() {{
                    put("$p", set("$x"));
                    put("$q", set("$y"));
                    put("$_0", set("$_0"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$p", set("$x", "$y"));
                    put("$q", set("$x"));
                    put("$_0", set("$_0"));
                }}
        );
        assertEquals(expected, result);
    }

    @Test
    public void relation_variable_role_unifies_many_to_many_rule_relation_roles() {
        String conjunction = "{ ($role1: $p, $role1: $q, $role2: $q) isa employment; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Relation queryConcludable = concludables.iterator().next().asRelation();

        Rule rule = createRule("two-people-are-employed-one-is-also-the-employer",
                               "{ $x isa person; $y isa person; }",
                               "(employee: $x, employer: $x, employee: $y) isa employment", logicMgr);

        FunctionalIterator<Unifier> unifier = queryConcludable.unify(rule.conclusion(), conceptMgr);
        Set<Map<String, Set<String>>> result = unifier.map(u -> getStringMapping(u.mapping())).toSet();

        Set<Map<String, Set<String>>> expected = set(
                new HashMap<String, Set<String>>() {{
                    put("$p", set("$x"));
                    put("$q", set("$x", "$y"));
                    put("$role1", set("$_employment:employee"));
                    put("$role2", set("$_employment:employer"));
                    put("$_0", set("$_0"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$p", set("$x"));
                    put("$q", set("$x", "$y"));
                    put("$role1", set("$_employment:employee", "$_employment:employer"));
                    put("$role2", set("$_employment:employee"));
                    put("$_0", set("$_0"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$p", set("$y"));
                    put("$q", set("$x"));
                    put("$role1", set("$_employment:employee", "$_employment:employer"));
                    put("$role2", set("$_employment:employee"));
                    put("$_0", set("$_0"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$p", set("$y"));
                    put("$q", set("$x"));
                    put("$role1", set("$_employment:employee"));
                    put("$role2", set("$_employment:employer"));
                    put("$_0", set("$_0"));
                }}
        );
        assertEquals(expected, result);
    }

    @Test
    public void relation_variable_role_unifies_many_to_many_rule_relation_roles_2() {
        String conjunction = "{ ($role1: $p, $role2: $q, $role1: $p) isa employment; }";

        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Relation queryConcludable = concludables.iterator().next().asRelation();

        Rule rule = createRule("two-people-are-employed-one-is-also-the-employer",
                               "{ $x isa person; $y isa person; }",
                               "(employee: $x, employer: $x, employee: $y) isa employment", logicMgr);

        FunctionalIterator<Unifier> unifier = queryConcludable.unify(rule.conclusion(), conceptMgr);
        Set<Map<String, Set<String>>> result = unifier.map(u -> getStringMapping(u.mapping())).toSet();

        Set<Map<String, Set<String>>> expected = set(
                new HashMap<String, Set<String>>() {{
                    put("$p", set("$x", "$y"));
                    put("$q", set("$x"));
                    put("$role1", set("$_employment:employee"));
                    put("$role2", set("$_employment:employer"));
                    put("$_0", set("$_0"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$p", set("$x", "$y"));
                    put("$q", set("$x"));
                    put("$role1", set("$_employment:employee", "$_employment:employer"));
                    put("$role2", set("$_employment:employee"));
                    put("$_0", set("$_0"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$p", set("$x"));
                    put("$q", set("$y"));
                    put("$role1", set("$_employment:employee", "$_employment:employer"));
                    put("$role2", set("$_employment:employee"));
                    put("$_0", set("$_0"));
                }}
        );
        assertEquals(expected, result);
    }

    @Test
    public void relation_duplicate_roles_unifies_rule_relation_distinct_roles() {
        String conjunction = "{ (employee: $p, employee: $p) isa employment; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Relation queryConcludable = concludables.iterator().next().asRelation();

        Rule rule = createRule("two-people-are-employed",
                               "{ $x isa person; $y isa person; $employment type employment; $employee type employment:employee; }",
                               "($employee: $x, $employee: $y) isa $employment", logicMgr);

        FunctionalIterator<Unifier> unifier = queryConcludable.unify(rule.conclusion(), conceptMgr);
        Set<Map<String, Set<String>>> result = unifier.map(u -> getStringMapping(u.mapping())).toSet();

        Set<Map<String, Set<String>>> expected = set(
                new HashMap<String, Set<String>>() {{
                    put("$p", set("$x", "$y"));
                    put("$_0", set("$_0"));
                }}
        );
        assertEquals(expected, result);
    }

    @Test
    public void relation_distinct_roles_unifies_rule_relation_duplicate_roles() {
        String conjunction = "{ (employee: $p, employee: $q) isa employment; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Relation queryConcludable = concludables.iterator().next().asRelation();

        Rule rule = createRule("a-person-is-employed-twice",
                               "{ $x isa person; $employment type employment; $employee type employment:employee; }",
                               "($employee: $x, $employee: $x) isa $employment", logicMgr);

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion(), conceptMgr).toList();
        Set<Map<String, Set<String>>> result = Iterators.iterate(unifiers).map(u -> getStringMapping(u.mapping())).toSet();

        Set<Map<String, Set<String>>> expected = set(
                new HashMap<String, Set<String>>() {{
                    put("$p", set("$x"));
                    put("$q", set("$x"));
                    put("$_0", set("$_0"));
                }}
        );
        assertEquals(expected, result);

        Unifier unifier = unifiers.get(0);
        // test requirements
        assertEquals(1, unifier.requirements().roleTypes().size());
        assertEquals(set(Label.of("employee", "employment"), Label.of("part-time-employee", "part-time-employment")),
                     unifier.requirements().roleTypes().get(Identifier.Variable.label("employment:employee")));
        assertEquals(1, unifier.requirements().isaExplicit().size());
        assertEquals(set(Label.of("employment"), Label.of("part-time-employment")),
                     unifier.requirements().isaExplicit().get(Identifier.Variable.anon(0)));
        assertEquals(0, unifier.requirements().predicates().size());

        // test filter allows a valid answer
        Relation employment = instanceOf("employment").asRelation();
        Thing person = instanceOf("person");
        addRolePlayer(employment, "employee", person);
        addRolePlayer(employment, "employee", person);
        Map<Identifier.Variable, Concept> concepts = map(
                pair(Identifier.Variable.anon(0), employment),
                pair(Identifier.Variable.name("x"), person),
                pair(Identifier.Variable.name("employment"), employment.getType()),
                pair(Identifier.Variable.name("employee"), employment.getType().getRelates("employee"))
        );
        Optional<ConceptMap> unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertTrue(unified.isPresent());
        assertEquals(3, unified.get().concepts().size());
        assertEquals(person, unified.get().get("p"));
        assertEquals(person, unified.get().get("q"));
        assertEquals(employment, unified.get().get(Identifier.Variable.anon(0)));
    }

    @Test
    public void relation_duplicate_roles_unifies_rule_relation_duplicate_roles() {
        String conjunction = "{ (employee: $p, employee: $p) isa employment; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Relation queryConcludable = concludables.iterator().next().asRelation();

        Rule rule = createRule("a-person-is-employed-twice",
                               "{ $x isa person; $employment type employment; $employee type employment:employee; }",
                               "($employee: $x, $employee: $x) isa $employment", logicMgr);

        FunctionalIterator<Unifier> unifier = queryConcludable.unify(rule.conclusion(), conceptMgr);
        Set<Map<String, Set<String>>> result = unifier.map(u -> getStringMapping(u.mapping())).toSet();

        Set<Map<String, Set<String>>> expected = set(
                new HashMap<String, Set<String>>() {{
                    put("$p", set("$x"));
                    put("$_0", set("$_0"));
                }}
        );
        assertEquals(expected, result);
    }

    @Test
    public void relation_more_players_than_rule_relation_fails_unify() {
        String conjunction = "{ (part-time-employee: $r, employer: $p, restriction: $q) isa part-time-employment; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Relation queryConcludable = concludables.iterator().next().asRelation();

        Rule rule = createRule("one-employee-one-employer",
                               "{ $x isa person; $y isa company; " +
                                       "$employee type employment:employee; $employer type employment:employer; }",
                               "($employee: $x, $employer: $y) isa employment", logicMgr);

        FunctionalIterator<Unifier> unifier = queryConcludable.unify(rule.conclusion(), conceptMgr);
        Set<Map<String, Set<String>>> result = unifier.map(u -> getStringMapping(u.mapping())).toSet();

        Set<Map<String, Set<String>>> expected = Collections.emptySet();
        assertEquals(expected, result);
    }


    // TODO: rule unification pruning tests based based on types
}
