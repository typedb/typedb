/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.logic.resolvable;

import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Label;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.ThingType;
import grakn.core.logic.LogicManager;
import grakn.core.logic.Rule;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import grakn.core.test.integration.util.Util;
import grakn.core.traversal.common.Identifier;
import graql.lang.Graql;
import org.junit.After;
import org.junit.Before;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.map;
import static grakn.common.collection.Collections.pair;
import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UnifyRelationConcludableTest {

    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("unify-relation-test");
    private static String database = "unify-relation-test";
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
        session = grakn.session(database, Arguments.Session.Type.SCHEMA);
        try (RocksTransaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
            tx.query().define(Graql.parseQuery("define " +
                                                       "person sub entity," +
                                                       "    owns first-name," +
                                                       "    owns last-name," +
                                                       "    owns age," +
                                                       "    plays employment:employee," +
                                                       "    plays part-time-employment:part-time-employee," +
                                                       "    plays friendship:friend;" +
                                                       "company sub entity," +
                                                       "    plays employment:employer;" +
                                                       "employment sub relation," +
                                                       "    relates employee," +
                                                       "    relates employer;" +
                                                       "part-time-employment sub employment," +
                                                       "    relates part-time-employee as employee;" +
                                                       "friendship sub relation," +
                                                       "    relates friend;" +
                                                       "name sub attribute, value string, abstract;" +
                                                       "first-name sub name;" +
                                                       "last-name sub name;" +
                                                       "age sub attribute, value long;" +
                                                       "").asDefine());
            tx.commit();
        }
        rocksTransaction = session.transaction(Arguments.Transaction.Type.READ);
        conceptMgr = rocksTransaction.concepts();
        logicMgr = rocksTransaction.logic();
    }

    @After
    public void tearDown() {
        grakn.close();
    }

    private Map<String, Set<String>> getStringMapping(Map<Identifier, Set<Identifier>> map) {
        return map.entrySet().stream().collect(Collectors.toMap(v -> v.getKey().toString(),
                                                                e -> e.getValue().stream().map(Identifier::toString).collect(Collectors.toSet()))
        );
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
        else throw GraknException.of(ILLEGAL_STATE);
    }

    private void addRolePlayer(Relation relation, String role, Thing player) {
        RelationType relationType = relation.getType();
        RoleType roleType = relationType.getRelates(role);
        assert roleType != null : "Role type " + role + " does not exist in relation type " + relation.getType().getLabel();
        relation.addPlayer(roleType, player);
    }


    private Conjunction parseConjunction(String query) {
        // TODO type resolver should probably run INSIDE the creation of a conclusion or concludable
        Conjunction conjunction = Disjunction.create(Graql.parsePattern(query).asConjunction().normalise()).conjunctions().iterator().next();
        return logicMgr.typeResolver().resolveLabels(conjunction);
    }

    private ThingVariable parseThingVariable(String blabla, String var) {
        // TODO placeholder
        return null;
    }

    private RelationConstraint findRelationConstraint(Conjunction conjunction) {
        List<RelationConstraint> rels = conjunction.variables().stream().flatMap(var -> var.constraints().stream())
                .filter(constraint -> constraint.isThing() && constraint.asThing().isRelation())
                .map(constraint -> constraint.asThing().asRelation()).collect(Collectors.toList());
        assert rels.size() == 1 : "More than 1 relation constraint in conjunction to search";
        return rels.get(0);
    }

    @Test
    public void relation_and_player_unifies() {
        String conjunction = "{ $r (employee: $y) isa employment; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Relation queryConcludable = concludables.iterator().next().asRelation();

        // rule: when { $x isa person; } then { (employee: $x) isa employment; }
        Conjunction whenExactRelation = parseConjunction("{ $x isa person; }");
        Conjunction thenExactRelation = parseConjunction("{ (employee: $x) isa employment; }");
        RelationConstraint thenEmploymentRelation = findRelationConstraint(thenExactRelation);
        Rule.Conclusion.Relation relationConclusion = Rule.Conclusion.Relation.create(thenEmploymentRelation, whenExactRelation.variables());

        List<Unifier> unifiers = queryConcludable.unify(relationConclusion, conceptMgr).collect(Collectors.toList());
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$y", set("$x"));
            put("$r", set("$_0"));
            put("$_employment:employee", set("$_employment:employee"));
            put("$_employment", set("$_employment"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(2, unifier.requirements().types().size());
        assertEquals(set(Label.of("employment"), Label.of("part-time-employment")),
                     unifier.requirements().types().get(Identifier.Variable.label("employment")));
        assertEquals(set(Label.of("employment:employee"), Label.of("part-time-employment:part-time-employee")),
                     unifier.requirements().types().get(Identifier.Variable.label("employment:employee")));
        assertEquals(0, unifier.requirements().isaExplicit().size());
        assertEquals(0, unifier.requirements().predicates().size());

        // test filter allows a valid answer
        Relation employment = instanceOf("employment").asRelation();
        Thing person = instanceOf("person");
        addRolePlayer(employment, "employee", person);
        Map<Identifier, Concept> identifiedConcepts = map(
                pair(Identifier.Variable.anon(0), employment),
                pair(Identifier.Variable.name("x"), person),
                pair(Identifier.Variable.label("employment"), employment.getType()),
                pair(Identifier.Variable.label("employment:employee"), employment.getType().getRelates("employee"))
        );
        Optional<ConceptMap> unified = unifier.unUnify(identifiedConcepts);
        assertTrue(unified.isPresent());
        assertEquals(2, unified.get().concepts().size());
        assertEquals(employment, unified.get().get("r"));
        assertEquals(person, unified.get().get("y"));

        // TODO enable after implement requirements
//        // filter out invalid types
//        Relation friendship = instanceOf("friendship").asRelation();
//        person = instanceOf("person");
//        addRolePlayer(employment, "friend", person);
//        identifiedConcepts = map(
//                pair(Identifier.Variable.anon(0), friendship),
//                pair(Identifier.Variable.name("x"), person),
//                pair(Identifier.Variable.label("employment"), friendship.getType()),
//                pair(Identifier.Variable.label("employee"), friendship.getType().getRelates("friend"))
//        );
//        unified = unifier.unUnify(identifiedConcepts);
//        assertFalse(unified.isPresent());
    }

    @Test
    public void relation_type_and_player_unifies() {
        String conjunction = "{ (employee: $y) isa $rel; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Relation queryConcludable = concludables.iterator().next().asRelation();

        // rule: when { $x isa person; } then { (employee: $x) isa employment; }
        Conjunction whenExactRelation = parseConjunction("{ $x isa person; }");
        Conjunction thenExactRelation = parseConjunction("{ (employee: $x) isa employment; }");
        RelationConstraint thenEmploymentRelation = findRelationConstraint(thenExactRelation);
        Rule.Conclusion.Relation relationConclusion = Rule.Conclusion.Relation.create(thenEmploymentRelation, whenExactRelation.variables());

        List<Unifier> unifiers = queryConcludable.unify(relationConclusion, conceptMgr).collect(Collectors.toList());
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$y", set("$x"));
            put("$rel", set("$_employment"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(2, unifier.requirements().types().size());
        assertEquals(set(Label.of("employment:employee"), Label.of("part-time-employment:part-time-employee")),
                     unifier.requirements().types().get(Identifier.Variable.label("employee")));
        assertEquals(0, unifier.requirements().isaExplicit().size());
        assertEquals(0, unifier.requirements().predicates().size());

        // test filter allows a valid answer
        Relation employment = instanceOf("employment").asRelation();
        Thing person = instanceOf("person");
        addRolePlayer(employment, "employee", person);
        Map<Identifier, Concept> identifiedConcepts = map(
                pair(Identifier.Variable.anon(0), employment),
                pair(Identifier.Variable.name("x"), person),
                pair(Identifier.Variable.label("employment"), employment.getType()),
                pair(Identifier.Variable.label("employee"), employment.getType().getRelates("employee"))
        );
        Optional<ConceptMap> unified = unifier.unUnify(identifiedConcepts);
        assertTrue(unified.isPresent());
        assertEquals(2, unified.get().concepts().size());
        assertEquals(employment.getType(), unified.get().get("rel"));
        assertEquals(person, unified.get().get("y"));

        // TODO enable after implement requirements
//        // filter out invalid types
//        Relation friendship = instanceOf("friendship").asRelation();
//        person = instanceOf("person");
//        addRolePlayer(employment, "friend", person);
//        identifiedConcepts = map(
//                pair(Identifier.Variable.anon(0), friendship),
//                pair(Identifier.Variable.name("x"), person),
//                pair(Identifier.Variable.label("employment"), friendship.getType()),
//                pair(Identifier.Variable.label("employee"), friendship.getType().getRelates("friend"))
//        );
//        unified = unifier.unUnify(identifiedConcepts);
//        assertFalse(unified.isPresent());
    }

    @Test
    public void relation_role_unifies() {
        String conjunction = "{ ($role: $x) isa employment; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Relation queryConcludable = concludables.iterator().next().asRelation();

        // rule: when { $x isa person; } then { (employee: $x) isa employment; }
        Conjunction whenExactRelation = parseConjunction("{ $x isa person; }");
        Conjunction thenExactRelation = parseConjunction("{ (employee: $x) isa employment; }");
        RelationConstraint thenEmploymentRelation = findRelationConstraint(thenExactRelation);
        Rule.Conclusion.Relation relationConclusion = Rule.Conclusion.Relation.create(thenEmploymentRelation, whenExactRelation.variables());

        List<Unifier> unifiers = queryConcludable.unify(relationConclusion, conceptMgr).collect(Collectors.toList());
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$x", set("$a"));
            put("$role", set("$_employee"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(2, unifier.requirements().types().size());
        assertEquals(set(Label.of("employment:employee"), Label.of("part-time-employment:part-time-employee")),
                     unifier.requirements().types().get(Identifier.Variable.label("employee")));
        assertEquals(0, unifier.requirements().isaExplicit().size());
        assertEquals(0, unifier.requirements().predicates().size());

        // test filter allows a valid answer
        Relation employment = instanceOf("employment").asRelation();
        Thing person = instanceOf("person");
        addRolePlayer(employment, "employee", person);
        Map<Identifier, Concept> identifiedConcepts = map(
                pair(Identifier.Variable.anon(0), employment),
                pair(Identifier.Variable.name("x"), person),
                pair(Identifier.Variable.label("employment"), employment.getType()),
                pair(Identifier.Variable.label("employee"), employment.getType().getRelates("employee"))
        );
        Optional<ConceptMap> unified = unifier.unUnify(identifiedConcepts);
        assertTrue(unified.isPresent());
        assertEquals(2, unified.get().concepts().size());
        assertEquals(employment.getType().getRelates("employee"), unified.get().get("role"));
        assertEquals(person, unified.get().get("y"));

        // TODO enable after implement requirements
//        // filter out invalid types
//        Relation friendship = instanceOf("friendship").asRelation();
//        person = instanceOf("person");
//        addRolePlayer(employment, "friend", person);
//        identifiedConcepts = map(
//                pair(Identifier.Variable.anon(0), friendship),
//                pair(Identifier.Variable.name("x"), person),
//                pair(Identifier.Variable.label("employment"), friendship.getType()),
//                pair(Identifier.Variable.label("employee"), friendship.getType().getRelates("friend"))
//        );
//        unified = unifier.unUnify(identifiedConcepts);
//        assertFalse(unified.isPresent());
    }

    @Test
    public void unify_relation_one_to_one_player() {
        String conjunction = "{ (employee: $x) isa employment; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction thenConjunction = parseConjunction("{ ($employee: $a) isa $employment; }");
        ThingVariable variable = parseThingVariable("$temp ($employee: $a) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable, conceptMgr);
        Set<Map<String, Set<String>>> result = unifier.map(u -> getStringMapping(u.mapping())).collect(Collectors.toSet());

        Set<Map<String, Set<String>>> expected = set(
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$a"));
                }}
        );
        assertEquals(expected, result);
    }

    @Test
    public void unify_relation_one_to_many() {
        String conjunction = "{ (employee: $x) isa employment; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction thenConjunction = parseConjunction(
                "{ ($employee: $a, $employee: $b, $employee: $c) isa $employment; }"
        );
        ThingVariable variable = parseThingVariable(
                "$temp ($employee: $a, $employee: $b, $employee: $c) isa $employment",
                "temp"
        );
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable, conceptMgr);
        Set<Map<String, Set<String>>> result = unifier.map(u -> getStringMapping(u.mapping())).collect(Collectors.toSet());

        Set<Map<String, Set<String>>> expected = set(
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$a"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$b"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$c"));
                }}
        );
        assertEquals(expected, result);
    }

    @Test
    public void unify_relation_many_to_many() {
        String conjunction = "{ (employee: $x, employee: $y) isa employment; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction thenConjunction = parseConjunction(
                "{ ($employee: $a, $employee: $b, $employee: $c) isa $employment; }");
        ThingVariable variable = parseThingVariable(
                "$temp ($employee: $a, $employee: $b, $employee: $c) isa $employment",
                "temp"
        );
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable, conceptMgr);
        Set<Map<String, Set<String>>> result = unifier.map(u -> getStringMapping(u.mapping())).collect(Collectors.toSet());

        Set<Map<String, Set<String>>> expected = set(
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$a"));
                    put("$y", set("$b"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$a"));
                    put("$y", set("$c"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$b"));
                    put("$y", set("$a"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$b"));
                    put("$y", set("$c"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$c"));
                    put("$y", set("$a"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$c"));
                    put("$y", set("$b"));
                }}
        );
        assertEquals(expected, result);
    }

    @Test
    public void relation_named_role_duplication() {
        String conjunction = "{ ($role: $x) isa employment; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction thenConjunction = parseConjunction("{ ($employee: $a, $employee: $b) isa $employment; }");
        ThingVariable variable = parseThingVariable("$temp ($employee: $a, $employee: $b) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable, conceptMgr);
        Set<Map<String, Set<String>>> result = unifier.map(u -> getStringMapping(u.mapping())).collect(Collectors.toSet());

        Set<Map<String, Set<String>>> expected = set(
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$a"));
                    put("$role", set("$employee"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$b"));
                    put("$role", set("$employee"));
                }}
        );
        assertEquals(expected, result);
    }

    @Test
    public void relation_repeated_players() {
        String conjunction = "{ (employee: $x, boss: $x) isa employment; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction thenConjunction = parseConjunction("{ ($employee: $a, $boss: $b) isa $employment; }");
        ThingVariable variable = parseThingVariable("$temp ($employee: $a, $employee: $b) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable, conceptMgr);
        Set<Map<String, Set<String>>> result = unifier.map(u -> getStringMapping(u.mapping())).collect(Collectors.toSet());

        Set<Map<String, Set<String>>> expected = set(
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$a", "$b"));
                }}
        );
        assertEquals(expected, result);
    }

    @Test
    public void relation_repeated_players_many_to_many() {
        String conjunction = "{ (employee: $x, boss: $x, employee: $y) isa employment; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction thenConjunction = parseConjunction("{$temp ($employee: $a, $boss: $a, $employee: $b) isa $employment; }");
        ThingVariable variable = parseThingVariable("$temp ($employee: $a, $boss: $a, $employee: $b) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable, conceptMgr);
        Set<Map<String, Set<String>>> result = unifier.map(u -> getStringMapping(u.mapping())).collect(Collectors.toSet());

        Set<Map<String, Set<String>>> expected = set(
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$a"));
                    put("$y", set("$b"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$a", "$b"));
                    put("$y", set("$a"));
                }}
        );
        assertEquals(expected, result);
    }

    @Test
    public void relation_repeated_players_many_to_many_roles() {
        String conjunction = "{ ($role1: $x, $role2: $y, $role1: $y) isa employment; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction thenConjunction = parseConjunction("{$temp ($employee: $a, $boss: $a, $employee: $b) isa $employment; }");
        ThingVariable variable = parseThingVariable("$temp ($employee: $a, $boss: $a, $employee: $b) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable, conceptMgr);
        Set<Map<String, Set<String>>> result = unifier.map(u -> getStringMapping(u.mapping())).collect(Collectors.toSet());

        Set<Map<String, Set<String>>> expected = set(
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$a"));
                    put("$y", set("$a", "$b"));
                    put("$role1", set("$employee"));
                    put("$role2", set("$boss"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$a"));
                    put("$y", set("$a", "$b"));
                    put("$role1", set("$employee", "$boss"));
                    put("$role2", set("$employee"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$b"));
                    put("$y", set("$a"));
                    put("$role1", set("$employee", "$boss"));
                    put("$role2", set("$employee"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$b"));
                    put("$y", set("$a"));
                    put("$role1", set("$employee"));
                    put("$role2", set("$boss"));
                }}
        );
        assertEquals(expected, result);
    }

    @Test
    public void relation_repeated_role_players() {
        String conjunction = "{ ($role1: $x, $role2: $y, $role1: $x) isa employment; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction thenConjunction = parseConjunction(
                "{$temp ($employee: $a, $boss: $a, $employee: $b) isa $employment; }"
        );
        ThingVariable variable = parseThingVariable(
                "$temp ($employee: $a, $boss: $a, $employee: $b) isa $employment", "temp"
        );
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable, conceptMgr);
        Set<Map<String, Set<String>>> result = unifier.map(u -> getStringMapping(u.mapping())).collect(Collectors.toSet());

        Set<Map<String, Set<String>>> expected = set(
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$a", "$b"));
                    put("$y", set("$a"));
                    put("$role1", set("$employee"));
                    put("$role2", set("$boss"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$a", "$b"));
                    put("$y", set("$a"));
                    put("$role1", set("$employee", "$boss"));
                    put("$role2", set("$employee"));
                }},
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$a"));
                    put("$y", set("$b"));
                    put("$role1", set("$employee", "$boss"));
                    put("$role2", set("$employee"));
                }}
        );
        assertEquals(expected, result);
    }

    @Test
    public void map_one_role_to_many() {
        String conjunction = "{ (employee: $x) isa employment; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction thenConjunction = parseConjunction("{$temp ($employee: $a, $employee: $a) isa $employment; }");
        ThingVariable variable = parseThingVariable("$temp ($employee: $a, $employee: $a) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable, conceptMgr);
        Set<Map<String, Set<String>>> result = unifier.map(u -> getStringMapping(u.mapping())).collect(Collectors.toSet());

        Set<Map<String, Set<String>>> expected = set(
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$a"));
                }}
        );
        assertEquals(expected, result);
    }

    @Test
    public void map_duplicate_roles_to_distinct_roles() {
        String conjunction = "{ (employee: $x, employee: $x) isa employment; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction thenConjunction = parseConjunction("{$temp ($employee: $a, $employee: $b) isa $employment; }");
        ThingVariable variable = parseThingVariable("$temp ($employee: $a, $employee: $b) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable, conceptMgr);
        Set<Map<String, Set<String>>> result = unifier.map(u -> getStringMapping(u.mapping())).collect(Collectors.toSet());

        Set<Map<String, Set<String>>> expected = set(
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$a", "$b"));
                }}
        );
        assertEquals(expected, result);
    }

    @Test
    public void map_distinct_roles_to_duplicate_roles() {
        String conjunction = "{ (employee: $x, employee: $y) isa employment; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction thenConjunction = parseConjunction("{$temp ($employee: $a, $employee: $a) isa $employment; }");
        ThingVariable variable = parseThingVariable("$temp ($employee: $a, $employee: $a) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable, conceptMgr);
        Set<Map<String, Set<String>>> result = unifier.map(u -> getStringMapping(u.mapping())).collect(Collectors.toSet());

        Set<Map<String, Set<String>>> expected = set(
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$a"));
                    put("$y", set("$a"));
                }}
        );
        assertEquals(expected, result);
    }

    @Test
    public void map_duplicate_roles_to_duplicate_roles() {
        String conjunction = "{ (employee: $x, employee: $x) isa employment; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction thenConjunction = parseConjunction("{$temp ($employee: $a, $employee: $a) isa $employment; }");
        ThingVariable variable = parseThingVariable("$temp ($employee: $a, $employee: $a) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable, conceptMgr);
        Set<Map<String, Set<String>>> result = unifier.map(u -> getStringMapping(u.mapping())).collect(Collectors.toSet());

        Set<Map<String, Set<String>>> expected = set(
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$a"));
                }}
        );
        assertEquals(expected, result);
    }

    @Test
    public void cannot_unify_more_specific_relation() {
        String conjunction = "{ (employee: $x, employer: $y, contract: $z) isa employment; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction thenConjunction = parseConjunction("{$temp ($employee: $a, $company: $b) isa $employment; }");
        ThingVariable variable = parseThingVariable("$temp ($employee: $a, $company: $b) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable, conceptMgr);
        Set<Map<String, Set<String>>> result = unifier.map(u -> getStringMapping(u.mapping())).collect(Collectors.toSet());

        Set<Map<String, Set<String>>> expected = Collections.emptySet();
        assertEquals(expected, result);
    }

}
