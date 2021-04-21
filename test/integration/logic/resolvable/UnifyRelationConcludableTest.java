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

import com.google.common.collect.Lists;
import com.vaticle.typedb.common.collection.Bytes;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.thing.Relation;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.concept.type.Type;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.rocks.RocksSession;
import com.vaticle.typedb.core.rocks.RocksTransaction;
import com.vaticle.typedb.core.rocks.RocksTypeDB;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.map;
import static com.vaticle.typedb.common.collection.Collections.pair;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
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

        try(RocksSession schemaSession = typedb.session(database, Arguments.Session.Type.SCHEMA)) {
            try (RocksTransaction tx = schemaSession.transaction(Arguments.Transaction.Type.WRITE)) {
                tx.query().define(TypeQL.parseQuery(
                        "define\n" +
                                                        "person sub entity,\n" +
                                                        "    owns first-name,\n" +
                                                        "    owns last-name,\n" +
                                                        "    owns age,\n" +
                                                        "    plays employment:employee,\n" +
                                                        "    plays employment:employer,\n" +
                                "   plays employment:employee-recommender,\n" +
                                "   plays friendship:friend;\n" +
                                "\n" +
                                "restricted-entity sub entity,\n" +
                                "   plays part-time-employment:restriction;\n" +
                                "student sub person,\n" +
                                                        "    plays part-time-employment:part-time-employee,\n" +
                                                        "    plays part-time-employment:part-time-employer,\n" +
                                "   plays part-time-employment:part-time-employee-recommender;\n" +
                                "\n" +
                                "student-driver sub student,\n" +
                                "   plays part-time-driving:night-shift-driver,\n" +
                                                        "plays part-time-driving:day-shift-driver;\n" +
                                "organisation sub entity,\n" +
                                                        "    plays employment:employer,\n" +
                                "  plays employment:employee,\n" +
                                "  plays employment:employee-recommender;\n" +
                                "part-time-organisation sub organisation,\n" +
                                "  plays part-time-employment:part-time-employer,\n" +
                                "  plays part-time-employment:part-time-employee,\n" +
                                "  plays part-time-employment:part-time-employee-recommender;\n" +
                                "driving-hire sub part-time-organisation,\n" +
                                "  plays part-time-driving:taxi,\n" +
                                "  plays part-time-driving:night-shift-driver,\n" +
                                "  plays part-time-driving:day-shift-driver;\n" +
                                "\n" +
                                                        "employment sub relation,\n" +
                                "  relates employer,\n" +
                                                        "    relates employee,\n" +
                                                        "    relates contractor,\n" +
                                "  relates employee-recommender;\n" +
                                "\n" +
                                                        "part-time-employment sub employment,\n" +
                                "  relates part-time-employer as employer,\n" +
                                "  relates part-time-employee as employee,\n" +
                                                        "    relates part-time-employee-recommender as employee-recommender,\n" +
                                                        "    relates restriction;\n" +
                                                        "\n " +
                                                        "    part-time-driving sub part-time-employment,\n" +
                                "  relates night-shift-driver as part-time-employee,\n" +
                                "  relates day-shift-driver as part-time-employee,\n" +
                                "  relates taxi as part-time-employer;\n" +
                                "\n" +
                                                        "friendship sub relation,\n" +
                                                        "    relates friend;\n" +
                                                        "name sub attribute, value string, abstract;\n" +
                                                        "first-name sub name;\n" +
                                                        "last-name sub name;\n" +
                                                        "age sub attribute, value long;"
                                                        ).asDefine());
                tx.commit();
            }
        }

        try(RocksSession dataSession = typedb.session(database, Arguments.Session.Type.DATA)){
            try (RocksTransaction tx = dataSession.transaction(Arguments.Transaction.Type.WRITE)) {
                tx.query().insert(TypeQL.parseQuery(
                        "insert " +
                                "(taxi: $x, night-shift-driver: $y) isa part-time-driving; " +
                                "(part-time-employer: $x, part-time-employee: $y, part-time-employee-recommender: $z) isa part-time-employment; " +
                                // note duplicate RP, needed to satisfy one of the child queries
                                "(taxi: $x, night-shift-driver: $x, part-time-employee-recommender: $z) isa part-time-driving; " +
                                "$x isa driving-hire;" +
                                "$y isa driving-hire;" +
                                "$z isa driving-hire;"
                        ).asInsert()
                );
                tx.commit();
            }
        }
        session = typedb.session(database, Arguments.Session.Type.SCHEMA);
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
        else if (type.isAttributeType()) {
            AttributeType atype = type.asAttributeType();
            if (atype.isString()) return type.asAttributeType().asString().put("john");
            else if (atype.isLong()) return type.asAttributeType().asLong().put(10L);
        }
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    private Set<Label> typeHierarchy(String type){
        return conceptMgr.getThingType(type).getSubtypes()
                .map(Type::getLabel).toSet();
    }

    private RoleType role(String roleType, String typeScope){
        return conceptMgr.getRelationType(typeScope)
                .getRelates()
                .filter(r -> r.getLabel().name().equals(roleType))
                .first().orElse(null);
    }

    private Set<Label> roleHierarchy(String roleType, String typeScope){
        return role(roleType, typeScope)
                .getSubtypes()
                .map(Type::getLabel)
                .toSet();
    }

    private void addRolePlayer(Relation relation, String role, Thing player) {
        RelationType relationType = relation.getType();
        RoleType roleType = relationType.getRelates(role);
        assert roleType != null : "Role type " + role + " does not exist in relation type " + relation.getType().getLabel();
        relation.addPlayer(roleType, player);
    }

    @Test
    public void relation_and_player_unifies_rule_relation_exact() {
        Unifier unifier = uniqueUnifier(
                "{ $r (employee: $y) isa employment; }",
                rule(
                        " (employee: $x) isa employment",
                "{ $x isa person; }")
        );
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$y", set("$x"));
            put("$r", set("$_0"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(
                typeHierarchy("employment"),
                unifier.requirements().isaExplicit().get(Variable.name("r")));
        assertEquals(1, unifier.requirements().isaExplicit().size());
        assertEquals(
                roleHierarchy("employee", "employment"),
                unifier.requirements().roleTypes().get(Variable.label("employment:employee")));
        assertEquals(1, unifier.requirements().roleTypes().size());
        assertEquals(0, unifier.requirements().predicates().size());

        // test filter allows a valid answer
        // code below tests unifier applied to an answer that is 1) satisfiable, 2) non-satisfiable
        Relation employment = instanceOf("employment").asRelation();
        Thing person = instanceOf("person");
        addRolePlayer(employment, "employee", person);
        Map<Variable, Concept> concepts = map(
                pair(Variable.anon(0), employment),
                pair(Variable.name("x"), person),
                pair(Variable.label("employment"), employment.getType()),
                pair(Variable.label("employment:employee"), employment.getType().getRelates("employee"))
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
                pair(Variable.anon(0), friendship),
                pair(Variable.name("x"), person),
                pair(Variable.label("employment"), friendship.getType()),
                pair(Variable.label("employment:employee"), friendship.getType().getRelates("friend"))
        );
        unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertFalse(unified.isPresent());
    }

    @Test
    public void relation_type_and_player_unifies_rule_relation_exact() {
        Unifier unifier = uniqueUnifier(
                "{ (employee: $y) isa $rel; }",
                rule(
                        "(employee: $x) isa employment",
                "{ $x isa person; }")
        );
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$y", set("$x"));
            put("$rel", set("$_employment"));
            put("$_0", set("$_0"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(
                roleHierarchy("employee", "employment"),
                unifier.requirements().roleTypes().get(Variable.label("relation:employee")));
        assertEquals(1, unifier.requirements().roleTypes().size());
        assertEquals(0, unifier.requirements().isaExplicit().size());
        assertEquals(0, unifier.requirements().predicates().size());

        // test filter allows a valid answer
        Relation employment = instanceOf("employment").asRelation();
        Thing person = instanceOf("person");
        addRolePlayer(employment, "employee", person);
        Map<Variable, Concept> concepts = map(
                pair(Variable.anon(0), employment),
                pair(Variable.name("x"), person),
                pair(Variable.label("employment"), employment.getType()),
                pair(Variable.label("employment:employee"), employment.getType().getRelates("employee"))
        );
        Optional<ConceptMap> unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertTrue(unified.isPresent());
        assertEquals(3, unified.get().concepts().size());
        assertEquals(employment.getType(), unified.get().get("rel"));
        assertEquals(person, unified.get().get("y"));
        assertEquals(employment, unified.get().get(Variable.anon(0)));

        // filter out invalid types
        Relation friendship = instanceOf("friendship").asRelation();
        person = instanceOf("person");
        addRolePlayer(friendship, "friend", person);
        concepts = map(
                pair(Variable.anon(0), friendship),
                pair(Variable.name("x"), person),
                pair(Variable.label("employment"), friendship.getType()),
                pair(Variable.label("employment:employee"), friendship.getType().getRelates("friend"))
        );
        unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertFalse(unified.isPresent());
    }

    @Test
    public void relation_role_unifies_rule_relation_exact() {
        Unifier unifier = uniqueUnifier(
                "{ ($role: $y) isa employment; }",
                rule(
                        " (employee: $x) isa employment ",
                "{ $x isa person; }")
        );
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$y", set("$x"));
            put("$role", set("$_employment:employee"));
            put("$_0", set("$_0"));
        }};
        assertEquals(expected, result);

        // test requirement
        assertEquals(
                typeHierarchy("employment"),
                unifier.requirements().isaExplicit().get(Variable.anon(0)));
        assertEquals(1, unifier.requirements().isaExplicit().size());
        assertEquals(1, unifier.requirements().isaExplicit().size());
        assertEquals(0, unifier.requirements().predicates().size());

        // test filter allows a valid answer
        Relation employment = instanceOf("employment").asRelation();
        Thing person = instanceOf("person");
        addRolePlayer(employment, "employee", person);
        Map<Variable, Concept> concepts = map(
                pair(Variable.anon(0), employment),
                pair(Variable.name("x"), person),
                pair(Variable.label("employment"), employment.getType()),
                pair(Variable.label("employment:employee"), employment.getType().getRelates("employee"))
        );
        Optional<ConceptMap> unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertTrue(unified.isPresent());
        assertEquals(3, unified.get().concepts().size());
        assertEquals(employment.getType().getRelates("employee"), unified.get().get("role"));
        assertEquals(person, unified.get().get("y"));
        assertEquals(employment, unified.get().get(Variable.anon(0)));

        // filter out invalid types
        Relation friendship = instanceOf("friendship").asRelation();
        person = instanceOf("person");
        addRolePlayer(friendship, "friend", person);
        concepts = map(
                pair(Variable.anon(0), friendship),
                pair(Variable.name("x"), person),
                pair(Variable.label("employment"), friendship.getType()),
                pair(Variable.label("employment:employee"), friendship.getType().getRelates("friend"))
        );
        unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertFalse(unified.isPresent());
    }

    @Test
    public void relation_without_isa_unifies_rule_relation() {
        Unifier unifier = uniqueUnifier(
                "{ (employee: $y); }",
                rule(
                        " (employee: $x) isa employment ",
                        "{ $x isa person; }")
        );
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$y", set("$x"));
            put("$_0", set("$_0"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(
                roleHierarchy("employee", "employment"),
                unifier.requirements().roleTypes().get(Variable.label("relation:employee")));
        assertEquals(1, unifier.requirements().roleTypes().size());
        assertEquals(0, unifier.requirements().isaExplicit().size());
        assertEquals(0, unifier.requirements().predicates().size());
    }

    @Test
    public void relation_duplicate_players_unifies_rule_relation_distinct_players() {
        List<Unifier> unifiers = unifiers(
                "{ (employee: $p, employee: $p) isa employment; }",
                rule(
                        "($employee: $x, $employee: $y) isa $employment",
                "{ $x isa person; $y isa person; $employment type employment;$employee type employment:employee; }")
        ).toList();
        Set<Map<String, Set<String>>> result = iterate(unifiers).map(u -> getStringMapping(u.mapping())).toSet();
        Set<Map<String, Set<String>>> expected = set(
                new HashMap<String, Set<String>>() {{
                    put("$p", set("$x", "$y"));
                    put("$_0", set("$_0"));
                }}
        );

        assertEquals(expected, result);

        Unifier unifier = unifiers.get(0);
        // test requirements
        assertEquals(
                typeHierarchy("employment"),
                unifier.requirements().isaExplicit().get(Variable.anon(0)));
        assertEquals(1, unifier.requirements().isaExplicit().size());
        assertEquals(
                roleHierarchy("employee", "employment"),
                unifier.requirements().roleTypes().get(Variable.label("employment:employee")));
        assertEquals(0, unifier.requirements().predicates().size());

        // test filter allows a valid answer
        Relation employment = instanceOf("employment").asRelation();
        Thing person = instanceOf("person");
        addRolePlayer(employment, "employee", person);
        addRolePlayer(employment, "employee", person);
        Map<Variable, Concept> identifiedConcepts = map(
                pair(Variable.anon(0), employment),
                pair(Variable.name("x"), person),
                pair(Variable.name("y"), person),
                pair(Variable.name("employment"), employment.getType()),
                pair(Variable.name("employee"), employment.getType().getRelates("employee"))
        );
        Optional<ConceptMap> unified = unifier.unUnify(identifiedConcepts, new Unifier.Requirements.Instance(map()));
        assertTrue(unified.isPresent());
        assertEquals(2, unified.get().concepts().size());
        assertEquals(person, unified.get().get("p"));

        // filter out answers with differing role players that must be the same
        employment = instanceOf("employment").asRelation();
        person = instanceOf("person");
        Thing differentPerson = instanceOf("person");
        addRolePlayer(employment, "employee", person);
        addRolePlayer(employment, "employee", differentPerson);
        identifiedConcepts = map(
                pair(Variable.anon(0), employment),
                pair(Variable.name("x"), person),
                pair(Variable.name("y"), differentPerson),
                pair(Variable.name("employment"), employment.getType()),
                pair(Variable.name("employee"), employment.getType().getRelates("employee"))
        );
        unified = unifier.unUnify(identifiedConcepts, new Unifier.Requirements.Instance(map()));
        assertFalse(unified.isPresent());
    }

    //TODO tests below do not test for requirements and unifier application to answers

    //[Single VARIABLE ROLE in parent]
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

    //[REFLEXIVE rule, Fewer roleplayers in parent]
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

    //[many2many]
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

    //[many2many]
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
    public void relation_unifies_many_to_many_rule_relation_players() {
        String conjunction = "{ (employee: $p, employer: $p, employee: $q) isa employment; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Relation queryConcludable = concludables.iterator().next().asRelation();

        Rule rule = createRule("two-people-are-employed-one-is-also-the-employer",
                               "{ $x isa person; $y isa person; }",
                               "(employee: $x, employer: $x, employee: $y) isa employment", logicMgr);

        List<Unifier> unifier = queryConcludable.unify(rule.conclusion(), conceptMgr).toList();
        List<Map<String, Set<String>>> result = iterate(unifier).map(u -> getStringMapping(u.mapping())).toList();

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

    //[multiple VARIABLE ROLE, many2many]
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

    //[multiple VARIABLE ROLE, many2many]
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

    //[reflexive parent]
    //TODO check answer satisfiability
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

    //[reflexive child]
    @Test
    public void relation_distinct_roles_unifies_rule_relation_duplicate_roles() {
        String conjunction = "{ (employee: $p, employee: $q) isa employment; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Relation queryConcludable = concludables.iterator().next().asRelation();

        Rule rule = createRule("a-person-is-employed-twice",
                               "{ $x isa person; $employment type employment; $employee type employment:employee; }",
                               "($employee: $x, $employee: $x) isa $employment", logicMgr);

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion(), conceptMgr).toList();
        Set<Map<String, Set<String>>> result = iterate(unifiers).map(u -> getStringMapping(u.mapping())).toSet();

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
            assertEquals(
                    roleHierarchy("employee", "employment"),
                    unifier.requirements().roleTypes().get(Variable.label("employment:employee")));
        assertEquals(
                typeHierarchy("employment"),
                unifier.requirements().isaExplicit().get(Variable.anon(0)));
        assertEquals(1, unifier.requirements().isaExplicit().size());
        assertEquals(0, unifier.requirements().predicates().size());

        // test filter allows a valid answer
        Relation employment = instanceOf("employment").asRelation();
        Thing person = instanceOf("person");
        addRolePlayer(employment, "employee", person);
        addRolePlayer(employment, "employee", person);
        Map<Variable, Concept> concepts = map(
                pair(Variable.anon(0), employment),
                pair(Variable.name("x"), person),
                pair(Variable.name("employment"), employment.getType()),
                pair(Variable.name("employee"), employment.getType().getRelates("employee"))
        );
        Optional<ConceptMap> unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertTrue(unified.isPresent());
        assertEquals(3, unified.get().concepts().size());
        assertEquals(person, unified.get().get("p"));
        assertEquals(person, unified.get().get("q"));
        assertEquals(employment, unified.get().get(Variable.anon(0)));
    }

    //[reflexive parent, child]
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
        String parentRelation = "{ (part-time-employee: $r, part-time-employer: $p, restriction: $q) isa part-time-employment; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(parentRelation, logicMgr));
        Concludable.Relation queryConcludable = concludables.iterator().next().asRelation();

        Rule rule = createRule("one-employee-one-employer",
                               "{ $x isa person; $y isa organisation; " +
                                       "$employee type employment:employee; $employer type employment:employer; }",
                               "($employee: $x, $employer: $y) isa employment", logicMgr);

        FunctionalIterator<Unifier> unifier = queryConcludable.unify(rule.conclusion(), conceptMgr);
        Set<Map<String, Set<String>>> result = unifier.map(u -> getStringMapping(u.mapping())).toSet();

        Set<Map<String, Set<String>>> expected = Collections.emptySet();
        assertEquals(expected, result);
    }

    @Test
    public void binaryRelationWithRoleHierarchy_ParentWithBaseRoles(){
        String parentRelation = "{ (employer: $x, employee: $y); }";
        String conclusion = "(part-time-employer: $u, part-time-employee: $v) isa part-time-employment";
        String conclusion2 = "(taxi: $u, night-shift-driver: $v) isa part-time-driving";

        verifyUnificationSucceeds(parentRelation, rule(conclusion, "{ $u isa part-time-organisation; $v isa student;}"));
        verifyUnificationSucceeds(parentRelation, rule(conclusion2, "{ $u isa driving-hire; $v isa student-driver;}"));
    }

    @Test
    public void binaryRelationWithRoleHierarchy_ParentWithSubRoles(){
        String parentRelation = "{ (part-time-employer: $x, part-time-employee: $y); }";
        String conclusion = "(part-time-employer: $u, part-time-employee: $v) isa part-time-employment";
        String conclusion2 = "(taxi: $u, night-shift-driver: $v) isa part-time-driving";
        String conclusion3 = "(taxi: $u, part-time-employee-recommender: $v) isa part-time-driving";
        String conclusion4 = "(employer: $u, employee: $v) isa employment";

        verifyUnificationSucceeds(parentRelation, rule(conclusion, "{$u isa part-time-organisation; $v isa student;}"));
        verifyUnificationSucceeds(parentRelation, rule(conclusion2, "{$u isa driving-hire; $v isa student-driver;}"));
        nonExistentUnifier(parentRelation, rule(conclusion3, "{$u isa driving-hire; $v isa student;}"));
        nonExistentUnifier(parentRelation, rule(conclusion4, "{$u isa part-time-organisation; $v isa person;}"));
    }

    @Test
    public void ternaryRelationWithRoleHierarchy_ParentWithBaseRoles(){
        String parentRelation = "{ (employer: $x, employee: $y, employee-recommender: $z); }";
        String conclusion = "(taxi: $u, night-shift-driver: $v, part-time-employee-recommender: $q) isa part-time-driving";
        String conclusion2 = "(part-time-employer: $u, part-time-employee: $v, part-time-employee-recommender: $q) isa part-time-employment";
        String conclusion3 = "(part-time-employer: $u, part-time-employer: $v, part-time-employee-recommender: $q) isa part-time-employment";

        verifyUnificationSucceeds(parentRelation, rule(conclusion, "{$u isa driving-hire; $v isa student-driver; $q isa student;}"));
        verifyUnificationSucceeds(parentRelation, rule(conclusion2, "{$u isa part-time-organisation; $v isa student; $q isa student;}"));
        nonExistentUnifier(parentRelation, rule(conclusion3, "{$u isa part-time-organisation; $v isa part-time-organisation; $q isa student;}"));
    }

    @Test
    public void ternaryRelationWithRoleHierarchy_ParentWithSubRoles(){
        String parentRelation = "{(part-time-employer: $x, part-time-employee: $y, part-time-employee-recommender: $z);}";
        String conclusion = "(employer: $u, employee: $v, employee-recommender: $q) isa employment";
        String conclusion2 = "(part-time-employer: $u, part-time-employee: $v, part-time-employee-recommender: $q) isa part-time-employment";
        String conclusion3 = "(taxi: $u, night-shift-driver: $v, part-time-employee-recommender: $q) isa part-time-driving";
        String conclusion4 = "(part-time-employer: $u, part-time-employer: $v, part-time-employee-recommender: $q) isa part-time-employment";

        nonExistentUnifier(parentRelation, rule(conclusion, "{$u isa organisation; $v isa person; $q isa person;}"));
        verifyUnificationSucceeds(parentRelation, rule(conclusion2, "{$u isa part-time-organisation; $v isa student; $q isa student;}"));
        verifyUnificationSucceeds(parentRelation, rule(conclusion3,"{$u isa driving-hire; $v isa student-driver; $q isa student;}"));
        nonExistentUnifier(parentRelation, rule(conclusion4, "{$u isa part-time-organisation; $v isa student; $q isa student;}"));
    }

    @Test
    public void ternaryRelationWithRoleHierarchy_ParentWithBaseRoles_childrenRepeatRolePlayers(){
        String parentRelation = "{ (employer: $x, employee: $y, employee-recommender: $z);}";
        String conclusion = "(employer: $u, employee: $u, employee-recommender: $q) isa employment";
        String conclusion2 = "(part-time-employer: $u, part-time-employee: $u, part-time-employee-recommender: $q) isa part-time-employment";
        String conclusion3 = "(part-time-employer: $u, part-time-employer: $u, part-time-employee-recommender: $q) isa part-time-employment";

        verifyUnificationSucceeds(parentRelation, rule(conclusion, "{$u isa student; $q isa student;}"));
        verifyUnificationSucceeds(parentRelation, rule(conclusion2,  "{$u isa student; $q isa student;}"));
        nonExistentUnifier(parentRelation, rule(conclusion3, "{$u isa student; $q isa student;}"));
    }

    @Test
    public void ternaryRelationWithRoleHierarchy_ParentWithBaseRoles_parentRepeatRolePlayers(){
        String parentRelation = "{ (employer: $x, employee: $x, employee-recommender: $y);}";
        String conclusion = "(employer: $u, employee: $v, employee-recommender: $q) isa employment";
        String conclusion2 = "(part-time-employer: $u, part-time-employee: $v, part-time-employee-recommender: $q) isa part-time-employment";
        String conclusion3 = "(part-time-employer: $u, part-time-employer: $v, part-time-employee-recommender: $q) isa part-time-employment";

        verifyUnificationSucceeds(parentRelation, rule(conclusion, "{$u isa student; $v isa student; $q isa student;}"));
        verifyUnificationSucceeds(parentRelation, rule(conclusion2, "{$u isa student; $v isa student; $q isa student;}"));
        nonExistentUnifier(parentRelation, rule(conclusion3, "{$u isa student; $v isa student; $q isa student;}"));
    }

    @Test
    public void binaryRelationWithRoleHierarchy_ParentHasFewerRelationPlayers() {
        String parent = "{ (part-time-employer: $x) isa employment; }";
        String parent2 = "{ (part-time-employee: $y) isa employment; }";
        String conclusion = "(part-time-employer: $y, part-time-employee: $x) isa part-time-employment";

        verifyUnificationSucceeds(parent, rule(conclusion,"{$x isa student; $y isa student;}"));
        verifyUnificationSucceeds(parent2, rule(conclusion, "{$x isa student; $y isa student;}"));
    }

    @Test
    public void relations_with_known_role_player_types() {
        //NB: typed roleplayers have match (indirect) semantics, they specify the type plus its specialisations
        List<String> parents = Lists.newArrayList(
                "{(employer: $x, employee: $y); $x isa organisation;}",
                "{(employer: $x, employee: $y); $x isa part-time-organisation;}",
                "{(employer: $x, employee: $y); $x isa driving-hire;}",
                //3
                "{(employer: $x, employee: $y); $y isa person;}",
                "{(employer: $x, employee: $y); $y isa student;}",
                "{(employer: $x, employee: $y); $y isa student-driver;}",
                //6
                "{(employer: $x, employee: $y); $x isa person;$y isa student;}",
                "{(employer: $x, employee: $y); $x isa person;$y isa student-driver;}",
                "{(employer: $x, employee: $y); $x isa person;$y isa driving-hire;}",
                //9
                "{(employer: $x, employee: $y); $x isa student;$y isa student-driver;}",
                "{(employer: $x, employee: $y); $x isa student;$y isa part-time-organisation;}",
                "{(employer: $x, employee: $y); $x isa student;$y isa driving-hire;}",
                //12
                "{(employer: $x, employee: $y); $x isa student-driver;$y isa driving-hire;}",
                "{(employer: $x, employee: $y); $x isa student-driver;$y isa student-driver;}"
        );
        List<String> conclusions = Lists.newArrayList(
                "(employer: $p, employee: $q) isa employment",
                "(employer: $p, employee: $p) isa employment",
                "(part-time-employer: $p, part-time-employee: $q) isa part-time-employment",
                "(taxi: $p, day-shift-driver: $q) isa part-time-driving"
        );
        verifyUnificationSucceedsFor(rule(conclusions.get(0), "{$p isa person; $q isa person;}"), parents, Lists.newArrayList(3, 4, 5, 6, 7, 9, 13));
        verifyUnificationSucceedsFor(rule(conclusions.get(0), "{$p isa student; $q isa student-driver;}"), parents, Lists.newArrayList(3, 4, 5, 6, 7, 9, 13));

        verifyUnificationSucceedsFor(rule(conclusions.get(1), "{$p isa person;}"), parents, Lists.newArrayList(3, 4, 5, 6, 7, 9, 13));
        verifyUnificationSucceedsFor(rule(conclusions.get(1), "{$p isa student;}"), parents, Lists.newArrayList(3, 4, 5, 6, 7, 9, 13));

        verifyUnificationSucceedsFor(rule(conclusions.get(2), "{$p isa part-time-organisation;$q isa student;}"), parents, Lists.newArrayList(0, 1 ,2, 3, 4, 5));
        verifyUnificationSucceedsFor(rule(conclusions.get(2), "{$p isa student;$q isa student-driver;}"), parents, Lists.newArrayList( 3, 4, 5, 6, 7, 9, 13));

        verifyUnificationSucceedsFor(rule(conclusions.get(3), "{$p isa driving-hire; $q isa student-driver;}"), parents, Lists.newArrayList(0, 1 ,2, 3, 4, 5));
        verifyUnificationSucceedsFor(rule(conclusions.get(3), "{$p isa driving-hire; $q isa student-driver;}"), parents, Lists.newArrayList(0, 1 ,2, 3, 4, 5));
    }

    @Test
    public void relations_with_exact_role_player_instances() {
        //NB: ids have insert (direct) semantics, they specify the exact type required (no specialisations)
        String personIID = Bytes.bytesToHexString(instanceOf("person").getIID());
        String studentIID = Bytes.bytesToHexString(instanceOf("student").getIID());
        String studentDriverIID = Bytes.bytesToHexString(instanceOf("student-driver").getIID());
        String organisationIID = Bytes.bytesToHexString(instanceOf("organisation").getIID());
        String ptOrganisationIID = Bytes.bytesToHexString(instanceOf("part-time-organisation").getIID());
        String drivingHireIID = Bytes.bytesToHexString(instanceOf("driving-hire").getIID());

        List<String> parents = Lists.newArrayList(
                String.format("{(employer: $x, employee: $y); $x iid %s;}", organisationIID),
                String.format("{(employer: $x, employee: $y); $x iid %s;}", ptOrganisationIID),
                String.format("{(employer: $x, employee: $y); $x iid %s;}", drivingHireIID),
                //3
                String.format("{(employer: $x, employee: $y); $y iid %s;}", personIID),
                String.format("{(employer: $x, employee: $y); $y iid %s;}", studentIID),
                String.format("{(employer: $x, employee: $y); $y iid %s;}", studentDriverIID),
                //6
                String.format("{(employer: $x, employee: $y); $x iid %s;$y iid %s;}", personIID, studentIID), //1-2
                String.format("{(employer: $x, employee: $y); $x iid %s;$y iid %s;}", personIID, studentDriverIID), //1-
                String.format("{(employer: $x, employee: $y); $x iid %s;$y iid %s;}", personIID, drivingHireIID),//1-3
                //9
                String.format("{(employer: $x, employee: $y); $x iid %s;$y iid %s;}", studentIID, studentDriverIID), //2-31
                String.format("{(employer: $x, employee: $y); $x iid %s;$y iid %s;}", studentIID, ptOrganisationIID), //2-2
                String.format("{(employer: $x, employee: $y); $x iid %s;$y iid %s;}", studentIID, drivingHireIID), //2-3
                //12
                String.format("{(employer: $x, employee: $y); $x iid %s;$y iid %s;}", studentDriverIID, drivingHireIID), //3-3
                String.format("{(employer: $x, employee: $y); $x iid %s;$y iid %s;}", studentDriverIID, studentDriverIID)
        );

        List<String> conclusions = Lists.newArrayList(
                "(employer: $p, employee: $q) isa employment",
                "(employer: $p, employee: $p) isa employment",
                "(part-time-employer: $p, part-time-employee: $q) isa part-time-employment",
                "(taxi: $p, day-shift-driver: $q) isa part-time-driving"
        );
        verifyUnificationSucceedsFor(rule(conclusions.get(0), "{$p isa person; $q isa person;}"), parents, Lists.newArrayList(3, 4, 5, 6, 7, 9, 13));
        verifyUnificationSucceedsFor(rule(conclusions.get(0), "{$p isa student; $q isa student-driver;}"), parents, Lists.newArrayList(5, 9, 13));

        //NB: all variants with different ids are only not satisfiable (and non-unifiable) if id types are disjoint
        verifyUnificationSucceedsFor(rule(conclusions.get(1), "{$p isa person;}"), parents, Lists.newArrayList(3, 4, 5, 6, 7, 9, 13));
        verifyUnificationSucceedsFor(rule(conclusions.get(1), "{$p isa student;}"), parents, Lists.newArrayList(4, 5, 9, 13 ));

        verifyUnificationSucceedsFor(rule(conclusions.get(2), "{$p isa part-time-organisation;$q isa student;}"), parents, Lists.newArrayList(1, 2, 4, 5));
        verifyUnificationSucceedsFor(rule(conclusions.get(2), "{$p isa student;$q isa student-driver;}"), parents, Lists.newArrayList(5, 9, 13));

        verifyUnificationSucceedsFor(rule(conclusions.get(3), "{$p isa driving-hire; $q isa student-driver;}"), parents, Lists.newArrayList(2, 5));
    }

    @Test
    public void relations_with_exact_relation_instance() {
        String employment = Bytes.bytesToHexString(instanceOf("employment").getIID());
        String ptEmployment = Bytes.bytesToHexString(instanceOf("part-time-employment").getIID());
        List<String> parents = Lists.newArrayList(
                String.format("{$r (employer: $x, employee: $y); $r iid %s;}", employment),
                String.format("{$r (employer: $x, employee: $y); $r iid %s;}", ptEmployment)
        );

        List<String> conclusions = Lists.newArrayList(
                "(employer: $p, employee: $q) isa employment",
                "(part-time-employer: $p, part-time-employee: $q) isa part-time-employment"
        );

        //NB: this is unifiable but not satisfiable
        verifyUnificationSucceedsFor(rule(conclusions.get(0), "{$p isa person; $q isa person;}"), parents, Lists.newArrayList(0));
        verifyUnificationSucceedsFor(rule(conclusions.get(1), "{$p isa part-time-organisation; $q isa student;}"), parents, Lists.newArrayList(1));
    }

    @Test
    public void relations_reflexive() {
        List<String> parents = Lists.newArrayList(
                "{(employer: $x, employee: $x); $x isa person;}",
                "{(part-time-employer: $x, part-time-employee: $x);}",
                "{(taxi: $x, employee: $x); $x isa organisation;}",
                "{(taxi: $x, employee: $x); $x isa driving-hire;}"
                );
        List<String> conclusions = Lists.newArrayList(
                "(employer: $p, employee: $q) isa employment",
                "(employer: $p, employee: $p) isa employment",
                "(part-time-employer: $p, part-time-employee: $q) isa part-time-employment",
                "(taxi: $p, day-shift-driver: $q) isa part-time-driving"
        );
        verifyUnificationSucceedsFor(rule(conclusions.get(0), "{$p isa student; $q isa student-driver;}"), parents, Lists.newArrayList(0));
        verifyUnificationSucceedsFor(rule(conclusions.get(1), "{$p isa student;}"), parents, Lists.newArrayList(0));

        //NB: here even though types are disjoint, the rule is unifiable
        verifyUnificationSucceedsFor(rule(conclusions.get(2), "{$p isa part-time-organisation;$q isa student;}"), parents, Lists.newArrayList(1));

        verifyUnificationSucceedsFor(rule(conclusions.get(2), "{$p isa student;$q isa student-driver;}"), parents, Lists.newArrayList(0, 1));

        verifyUnificationSucceedsFor(rule(conclusions.get(3), "{$p isa driving-hire; $q isa student-driver;}"), parents,  Lists.newArrayList(1));
        verifyUnificationSucceedsFor(rule(conclusions.get(3), "{$p isa driving-hire; $q isa driving-hire;}"), parents, Lists.newArrayList(1, 2, 3));
    }

    private Rule rule(String conclusion, String conditions){
        return createRule(UUID.randomUUID().toString(), conditions, conclusion, logicMgr);
    }

    private FunctionalIterator<Unifier> unifiers(String parent, Rule rule){
        Conjunction parentConjunction = resolvedConjunction(parent, logicMgr);
        Concludable.Relation queryConcludable = Concludable.create(parentConjunction).stream()
                .filter(Concludable::isRelation)
                .map(Concludable::asRelation)
                .findFirst().orElse(null);
        return queryConcludable.unify(rule.conclusion(), conceptMgr);
    }

    private void nonExistentUnifier(String parent, Rule rule){
        assertFalse(unifiers(parent, rule).hasNext());
    }

    private Unifier uniqueUnifier(String parent, Rule rule){
        List<Unifier> unifiers = unifiers(parent, rule).toList();
        assertEquals(1, unifiers.size());
        return unifiers.iterator().next();
    }

    private void verifyUnificationSucceedsFor(Rule rule, List<String> parents, List<Integer> unifiableParents){
        for (int parentIndex = 0; parentIndex < parents.size() ; parentIndex++) {
            String parent = parents.get(parentIndex);
            assertEquals(
                    String.format("Unexpected unification outcome at index [%s]:\nconjunction: %s\nconclusion: %s\nconditions: %s\n",
                            parentIndex, parent, rule.conclusion(), rule.condition()),
                    unifiableParents.contains(parentIndex), unifiers(parent, rule).hasNext()
            );
        }
    }

    private void verifyUnificationSucceeds(String parent, Rule rule){
        Unifier unifier = uniqueUnifier(parent, rule);
        List<ConceptMap> childAnswers = rocksTransaction.query().match(TypeQL.match(rule.getThenPreNormalised())).toList();
        List<ConceptMap> parentAnswers = rocksTransaction.query().match(TypeQL.match(TypeQL.parsePattern(parent))).toList();
        assertFalse(childAnswers.isEmpty());
        assertFalse(parentAnswers.isEmpty());

        List<ConceptMap> unifiedAnswers = childAnswers.stream()
                .map(ans -> {
                    Map<Variable, Concept> labelledTypes = addRequiredLabeledTypes(ans, unifier);
                    Map<Variable, Concept> requiredRetrievableConcepts = addRequiredRetrievableConcepts(ans, unifier);
                    labelledTypes.putAll(requiredRetrievableConcepts);
                    //TODO if want to use with iids add instance requirements
                    ConceptMap unified = unifier.unUnify(labelledTypes, new Unifier.Requirements.Instance(map())).orElse(null);
                    if (unified == null) return null;
                    Map<Variable.Retrievable, Concept> concepts = unified.concepts().entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    requiredRetrievableConcepts.forEach(concepts::remove);
                    return new ConceptMap(concepts);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        assertFalse(unifiedAnswers.isEmpty());
        assertTrue(parentAnswers.containsAll(unifiedAnswers));
    }

    Map<Variable, Concept> addRequiredLabeledTypes(ConceptMap ans, Unifier unifier){
        Map<Variable, Concept> imap = new HashMap<>(ans.concepts());
        unifier.unifiedRequirements().roleTypes()
                .forEach((var, labels) -> labels.forEach(label -> imap.put(var, role(label.name(), label.scope().get()))));
        return imap;
    }

    Map<Variable, Concept> addRequiredRetrievableConcepts(ConceptMap ans, Unifier unifier){
        //insert random concepts for any var in unifier that is not in conceptmap
        Iterator<? extends Thing> instances = iterate(conceptMgr.getRootThingType().getInstances());
        return unifier.reverseUnifier().keySet().stream()
                .filter(var -> !ans.contains(var.asRetrievable()))
                .collect(Collectors.toMap(var -> var, var -> instances.next()));
    }
}
