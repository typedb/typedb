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

import grakn.core.Grakn.Transaction;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Label;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.ThingType;
import grakn.core.logic.LogicManager;
import grakn.core.logic.Rule;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import grakn.core.test.integration.util.Util;
import grakn.core.traversal.common.Identifier;
import graql.lang.Graql;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.common.collection.Collections.map;
import static grakn.common.collection.Collections.pair;
import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UnifyHasConcludableTest {

    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("unify-isa-test");
    private static String database = "unify-isa-test";
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
                                                       "self-owning-attribute sub attribute, value long, owns self-owning-attribute;" +
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
        assert type != null;
        if (type.isEntityType()) return type.asEntityType().create();
        else if (type.isRelationType()) return type.asRelationType().create();
        else if (type.isAttributeType() && type.asAttributeType().isString())
            return type.asAttributeType().asString().put("john");
        else if (type.isAttributeType() && type.asAttributeType().isLong())
            return type.asAttributeType().asLong().put(10L);
        else throw GraknException.of(ILLEGAL_STATE);
    }

    private Thing instanceOf(String stringAttributeLabel, String stringValue) {
        AttributeType type = conceptMgr.getAttributeType(stringAttributeLabel);
        assert type != null;
        return type.asString().put(stringValue);
    }

    private Conjunction parseConjunction(String query) {
        // TODO type resolver should probably run INSIDE the creation of a conclusion or concludable
        Conjunction conjunction = Disjunction.create(Graql.parsePattern(query).asConjunction().normalise()).conjunctions().iterator().next();
        return logicMgr.typeResolver().resolveLabels(conjunction);
    }

    private Rule createRule(String label, String whenConjunctionPattern, String thenThingPattern) {
        try (Transaction txn = session.transaction(Arguments.Transaction.Type.WRITE)) {
            Rule rule = logicMgr.putRule(label, Graql.parsePattern(whenConjunctionPattern).asConjunction(),
                                         Graql.parseVariable(thenThingPattern).asThing());
            txn.commit();
            return rule;
        }
    }

    //TODO: create more tests when type inference is working to test unifier pruning

    @Test
    public void has_attribute_exact_unifies_rule_has_exact() {
        String conjunction = "{ $y has name 'john'; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Has queryConcludable = concludables.iterator().next().asHas();

        Rule rule = createRule("has-rule", "{ $x isa person; }", "$x has first-name 'john'");

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion().asHas(), conceptMgr).toList();
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$y", set("$x"));
            put("$_0", set("$_0"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(0, unifier.requirements().types().size());
        assertEquals(1, unifier.requirements().isaExplicit().size());
        assertEquals(set(Label.of("name"), Label.of("first-name"), Label.of("last-name")), unifier.requirements().isaExplicit().values().iterator().next());
        assertEquals(1, unifier.requirements().predicates().size());

        // test filter allows a valid answer
        Map<Identifier, Concept> identifiedConcepts = map(
                pair(Identifier.Variable.name("x"), instanceOf("person")),
                pair(Identifier.Variable.anon(0), instanceOf("first-name", "john"))
        );
        Optional<ConceptMap> unified = unifier.unUnify(identifiedConcepts);
        assertTrue(unified.isPresent());
        assertEquals(1, unified.get().concepts().size());

        // filter out invalid type
        identifiedConcepts = map(
                pair(Identifier.Variable.name("x"), instanceOf("person")),
                pair(Identifier.Variable.anon(0), instanceOf("age"))
        );
        unified = unifier.unUnify(identifiedConcepts);
        assertFalse(unified.isPresent());

        // filter out invalid value
        identifiedConcepts = map(
                pair(Identifier.Variable.name("x"), instanceOf("person")),
                pair(Identifier.Variable.anon(0), instanceOf("first-name", "bob"))
        );
        unified = unifier.unUnify(identifiedConcepts);
        assertFalse(unified.isPresent());
    }

    @Test
    public void has_attribute_exact_unifies_rule_has_variable() {
        String conjunction = "{ $y has name 'john'; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Has queryConcludable = concludables.iterator().next().asHas();

        Rule rule = createRule("has-rule", "{ $x isa person; $a isa first-name; }", "$x has $a");

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion().asHas(), conceptMgr).toList();
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        HashMap<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$y", set("$x"));
            put("$_0", set("$a"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(0, unifier.requirements().types().size());
        assertEquals(1, unifier.requirements().isaExplicit().size());
        assertEquals(set(Label.of("name"), Label.of("first-name"), Label.of("last-name")), unifier.requirements().isaExplicit().values().iterator().next());
        assertEquals(1, unifier.requirements().predicates().size());

        // test filter allows a valid answer
        Map<Identifier, Concept> identifiedConcepts = map(
                pair(Identifier.Variable.name("x"), instanceOf("person")),
                pair(Identifier.Variable.name("a"), instanceOf("last-name", "john"))
        );
        Optional<ConceptMap> unified = unifier.unUnify(identifiedConcepts);
        assertTrue(unified.isPresent());
        assertEquals(1, unified.get().concepts().size());

        // filter out invalid type
        identifiedConcepts = map(
                pair(Identifier.Variable.name("x"), instanceOf("person")),
                pair(Identifier.Variable.name("a"), instanceOf("age"))
        );
        unified = unifier.unUnify(identifiedConcepts);
        assertFalse(unified.isPresent());

        // filter out invalid value
        identifiedConcepts = map(
                pair(Identifier.Variable.name("x"), instanceOf("person")),
                pair(Identifier.Variable.name("a"), instanceOf("first-name", "bob"))
        );
        unified = unifier.unUnify(identifiedConcepts);
        assertFalse(unified.isPresent());
    }

    @Ignore
    @Test
    public void has_attribute_exact_prunes_irrelevant_rules() {
        // TODO implement a test for unifier pruning, will require type hinting
    }


    @Test
    public void has_attribute_variable_unifies_rule_has_exact() {
        String conjunction = "{ $y has $a; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Has queryConcludable = concludables.iterator().next().asHas();

        Rule rule = createRule("has-rule", "{ $x isa person; }", "$x has first-name \"john\"");

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion().asHas(), conceptMgr).toList();
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$y", set("$x"));
            put("$a", set("$_0"));
        }};
        assertEquals(expected, result);

        // test unifier allows a valid answer
        Map<Identifier, Concept> identifiedConcepts = map(
                pair(Identifier.Variable.name("x"), instanceOf("person")),
                pair(Identifier.Variable.anon(0), instanceOf("first-name", "john"))
        );
        Optional<ConceptMap> unified = unifier.unUnify(identifiedConcepts);
        assertTrue(unified.isPresent());
        assertEquals(2, unified.get().concepts().size());

        // test requirements
        assertEquals(0, unifier.requirements().types().size());
        assertEquals(0, unifier.requirements().isaExplicit().size());
        assertEquals(0, unifier.requirements().predicates().size());
    }

    @Test
    public void has_attribute_variable_unifies_rule_has_variable() {
        String conjunction = "{ $y has $b; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Has queryConcludable = concludables.iterator().next().asHas();

        Rule rule = createRule("has-rule", "{ $x isa person; $a isa first-name; }", "$x has $a");

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion().asHas(), conceptMgr).toList();
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        HashMap<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$y", set("$x"));
            put("$b", set("$a"));
        }};
        assertEquals(expected, result);

        // test filter allows a valid answer
        Map<Identifier, Concept> identifiedConcepts = map(
                pair(Identifier.Variable.name("x"), instanceOf("person")),
                pair(Identifier.Variable.name("a"), instanceOf("last-name", "john"))
                );
        Optional<ConceptMap> unified = unifier.unUnify(identifiedConcepts);
        assertTrue(unified.isPresent());
        assertEquals(2, unified.get().concepts().size());

        // test requirements
        assertEquals(0, unifier.requirements().types().size());
        assertEquals(0, unifier.requirements().isaExplicit().size());
        assertEquals(0, unifier.requirements().predicates().size());
    }

    @Test
    public void has_attribute_typed_variable_unifies_rule_has_exact() {
        String conjunction = "{ $y has name $b; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Has queryConcludable = concludables.iterator().next().asHas();

        Rule rule = createRule("has-rule", "{ $x isa person; }", "$x has first-name \"john\"");

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion().asHas(), conceptMgr).toList();
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$y", set("$x"));
            put("$b", set("$_0"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(0, unifier.requirements().types().size());
        assertEquals(1, unifier.requirements().isaExplicit().size());
        assertEquals(set(Label.of("name"), Label.of("first-name"), Label.of("last-name")), unifier.requirements().isaExplicit().values().iterator().next());
        assertEquals(0, unifier.requirements().predicates().size());

        // test filter allows a valid answer
        Map<Identifier, Concept> identifiedConcepts = map(
                pair(Identifier.Variable.name("x"), instanceOf("person")),
                pair(Identifier.Variable.anon(0), instanceOf("first-name", "john"))
        );
        Optional<ConceptMap> unified = unifier.unUnify(identifiedConcepts);
        assertTrue(unified.isPresent());
        assertEquals(2, unified.get().concepts().size());

        // filter out invalid type
        identifiedConcepts = map(
                pair(Identifier.Variable.name("x"), instanceOf("person")),
                pair(Identifier.Variable.anon(0), instanceOf("age"))
        );
        unified = unifier.unUnify(identifiedConcepts);
        assertFalse(unified.isPresent());
    }

    @Test
    public void has_attribute_typed_variable_unifies_rule_has_variable() {
        String conjunction = "{ $y has name $b; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Has queryConcludable = concludables.iterator().next().asHas();

        Rule rule = createRule("has-rule", "{ $x isa person; $a isa first-name; }", "$x has $a");

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion().asHas(), conceptMgr).toList();
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        HashMap<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$y", set("$x"));
            put("$b", set("$a"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(0, unifier.requirements().types().size());
        assertEquals(1, unifier.requirements().isaExplicit().size());
        assertEquals(set(Label.of("name"), Label.of("first-name"), Label.of("last-name")), unifier.requirements().isaExplicit().values().iterator().next());
        assertEquals(0, unifier.requirements().predicates().size());

        // test filter allows a valid answer
        Map<Identifier, Concept> identifiedConcepts = map(
                pair(Identifier.Variable.name("x"), instanceOf("person")),
                pair(Identifier.Variable.name("a"), instanceOf("first-name", "john"))
        );
        Optional<ConceptMap> unified = unifier.unUnify(identifiedConcepts);
        assertTrue(unified.isPresent());
        assertEquals(2, unified.get().concepts().size());

        // filter out invalid type
        identifiedConcepts = map(
                pair(Identifier.Variable.name("x"), instanceOf("person")),
                pair(Identifier.Variable.name("a"), instanceOf("age"))
        );
        unified = unifier.unUnify(identifiedConcepts);
        assertFalse(unified.isPresent());
    }

    @Test
    public void has_many_to_one_unifier() {
        String conjunction = "{ $x has attribute $y; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Has queryConcludable = concludables.iterator().next().asHas();

        Rule rule = createRule("has-rule", "{ $a isa self-owning-attribute; }", "$a has $a");

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion().asHas(), conceptMgr).toList();
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$x", set("$a"));
            put("$y", set("$a"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(0, unifier.requirements().types().size());
        assertEquals(1, unifier.requirements().isaExplicit().size());
        assertEquals(0, unifier.requirements().predicates().size());
    }

    @Test
    public void has_one_to_many_unifier() {
        String conjunction = "{ $b has attribute $b; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Has queryConcludable = concludables.iterator().next().asHas();

        Rule rule = createRule("has-rule", "{ $x isa person; }", "$x has first-name \"john\"");

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion().asHas(), conceptMgr).toList();
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$b", set("$x", "$_0"));
        }};
        assertEquals(expected, result);

        // test requirements of one-to-many using valid answer (would never actually come from rule!)
        Map<Identifier, Concept> identifiedConcepts = map(
                pair(Identifier.Variable.name("x"), instanceOf("first-name", "john")), // nonsense on purpose!!
                pair(Identifier.Variable.anon(0), instanceOf("first-name", "john"))
        );
        Optional<ConceptMap> unified = unifier.unUnify(identifiedConcepts);
        assertTrue(unified.isPresent());
        assertEquals(1, unified.get().concepts().size());

        // test requirements of one-to-many using invalid answer
        identifiedConcepts = map(
                pair(Identifier.Variable.name("x"), instanceOf("person")),
                pair(Identifier.Variable.anon(0), instanceOf("age"))
        );
        unified = unifier.unUnify(identifiedConcepts);
        assertFalse(unified.isPresent());
    }

    @Test
    public void has_all_equivalent_vars_unifier() {
        String conjunction = "{ $b has self-owning-attribute $b; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Has queryConcludable = concludables.iterator().next().asHas();

        Rule rule = createRule("has-rule", "{ $a isa self-owning-attribute; }", "$a has $a");

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion().asHas(), conceptMgr).toList();
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$b", set("$a"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(0, unifier.requirements().types().size());
        assertEquals(1, unifier.requirements().isaExplicit().size());
        assertEquals(set(Label.of("self-owning-attribute")), unifier.requirements().isaExplicit().values().iterator().next());
        assertEquals(0, unifier.requirements().predicates().size());
    }
}
