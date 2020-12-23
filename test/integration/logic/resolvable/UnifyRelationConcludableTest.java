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
import grakn.core.concept.ConceptManager;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.ThingType;
import grakn.core.logic.LogicManager;
import grakn.core.logic.Rule;
import grakn.core.logic.transformer.Unifier;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.pattern.constraint.thing.HasConstraint;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static org.junit.Assert.assertEquals;
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
                                                       "    plays employment:employee;" +
                                                       "company sub entity," +
                                                       "    plays employment:employer;" +
                                                       "employment sub relation," +
                                                       "    relates employee," +
                                                       "    relates employer;" +
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

    private Conjunction parseConjunction(String query) {
        // TODO type resolver should probably run INSIDE the creation of a conclusion or concludable
        Conjunction conjunction = Disjunction.create(Graql.parsePattern(query).asConjunction().normalise()).conjunctions().iterator().next();
        return logicMgr.typeResolver().resolveLabels(conjunction);
    }

    private ThingVariable parseThingVariable(String blabla, String var) {
        // TODO placeholderb
        return null;
    }


    @Test
    public void relation_named_role() {
        String conjunction = "{ ($role: $x) isa employment; }";
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
                    put("$role", set("$employee"));
                }}
        );
        assertEquals(expected, result);
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
    public void has_duplicate_vars_conj() {
        String conjunction = "{ $x has name $x; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Has conjConcludable = concludables.iterator().next().asHas();

        Conjunction thenConjunction = parseConjunction("{ $p isa $person; $p has $name; $name = 'bob' isa name;}");
        ThingVariable variable = parseThingVariable("$p has $name", "p");
        HasConstraint hasConstraint = variable.has().iterator().next();
        Rule.Conclusion.Has hasConcludable = new Rule.Conclusion.Has(hasConstraint, thenConjunction.variables());

        Optional<Unifier> unifier = conjConcludable.unify(hasConcludable, conceptMgr).findFirst();
        assertTrue(unifier.isPresent());
        Map<String, Set<String>> result = getStringMapping(unifier.get().mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$x", set("$p", "$name"));
        }};
        assertTrue(result.entrySet().containsAll(expected.entrySet()));
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
    public void relation_match_owner() {
        String conjunction = "{ $r (employee: $x) isa employment; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction thenConjunction = parseConjunction("{$temp ($employee: $a) isa $employment; }");
        ThingVariable variable = parseThingVariable("$temp ($employee: $a) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable, conceptMgr);
        Set<Map<String, Set<String>>> result = unifier.map(u -> getStringMapping(u.mapping())).collect(Collectors.toSet());

        Set<Map<String, Set<String>>> expected = set(
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$a"));
                    put("$r", set("$temp"));
                }}
        );
        assertEquals(expected, result);
    }

    @Test
    public void relation_match_relation() {
        String conjunction = "{ (employee: $x) isa $rel; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction thenConjunction = parseConjunction("{$temp ($employee: $a) isa $employment; }");
        ThingVariable variable = parseThingVariable("$temp ($employee: $a) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable, conceptMgr);
        Set<Map<String, Set<String>>> result = unifier.map(u -> getStringMapping(u.mapping())).collect(Collectors.toSet());

        Set<Map<String, Set<String>>> expected = set(
                new HashMap<String, Set<String>>() {{
                    put("$x", set("$a"));
                    put("$rel", set("$employment"));
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
