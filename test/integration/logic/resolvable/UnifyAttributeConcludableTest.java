/*
 * Copyright (C) 2021 Grakn Labs
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

import grakn.core.common.iterator.Iterators;
import grakn.core.common.parameters.Arguments;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.AttributeType;
import grakn.core.logic.LogicManager;
import grakn.core.logic.Rule;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.pattern.variable.Variable;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import grakn.core.test.integration.util.Util;
import grakn.core.traversal.common.Identifier;
import graql.lang.Graql;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UnifyAttributeConcludableTest {

    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("unify-attribute-test");
    private static String database = "unify-attribute-test";
    private static RocksGrakn grakn;
    private static RocksSession session;
    private static RocksTransaction rocksTransaction;
    private static ConceptManager conceptMgr;
    private static LogicManager logicMgr;

    @BeforeClass
    public static void setUp() throws IOException {
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
    }

    @AfterClass
    public static void tearDown() {
        session.close();
        grakn.close();
    }

    @Before
    public void setUpTransaction() {
        rocksTransaction = session.transaction(Arguments.Transaction.Type.WRITE);
        conceptMgr = rocksTransaction.concepts();
        logicMgr = rocksTransaction.logic();
    }

    @After
    public void tearDownTransaction() { rocksTransaction.close(); }

    private Map<String, Set<String>> getStringMapping(Map<Identifier, Set<Identifier>> map) {
        return map.entrySet().stream().collect(Collectors.toMap(v -> v.getKey().toString(),
                                                                e -> e.getValue().stream().map(Identifier::toString).collect(Collectors.toSet()))
        );
    }

    private Thing instanceOf(String stringAttributeLabel, String stringValue) {
        AttributeType type = conceptMgr.getAttributeType(stringAttributeLabel);
        assert type != null;
        return type.asString().put(stringValue);
    }

    private Conjunction resolvedConjunction(String query) {
        Conjunction conjunction = Disjunction.create(Graql.parsePattern(query).asConjunction().normalise()).conjunctions().iterator().next();
        logicMgr.typeResolver().resolve(conjunction);
        return conjunction;
    }

    private Rule createRule(String label, String whenConjunctionPattern, String thenThingPattern) {
        Rule rule = logicMgr.putRule(label, Graql.parsePattern(whenConjunctionPattern).asConjunction(),
                                     Graql.parseVariable(thenThingPattern).asThing());
        return rule;
    }

    @Test
    public void literal_predicates_unify_and_filter_answers() {
        String conjunction = "{ $a = 'john'; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction));
        Concludable.Attribute queryConcludable = concludables.iterator().next().asAttribute();

        Rule rule = createRule("isa-rule", "{ $x isa person; }", "$x has first-name \"john\"");

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion(), conceptMgr).toList();
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$a", set("$_0"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(0, unifier.constraintRequirements().types().size());
        assertEquals(0, unifier.constraintRequirements().isaExplicit().size());
        assertEquals(1, unifier.constraintRequirements().predicates().size());

        // test filter allows a valid answer
        Map<Identifier, Concept> identifiedConcepts = map(
                pair(Identifier.Variable.anon(0), instanceOf("first-name", "john"))
        );
        Optional<ConceptMap> unified = unifier.unUnify(identifiedConcepts, new Unifier.Requirements.Instance(map()));
        assertTrue(unified.isPresent());
        assertEquals(1, unified.get().concepts().size());

        identifiedConcepts = map(
                pair(Identifier.Variable.anon(0), instanceOf("first-name", "abe"))
        );
        unified = unifier.unUnify(identifiedConcepts, new Unifier.Requirements.Instance(map()));
        assertFalse(unified.isPresent());

    }

    @Test
    public void variable_predicates_unify_value_conclusions() {
        String conjunction = "{ $x > $y; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction));
        assertEquals(2, concludables.size());
        Concludable.Attribute queryConcludable = concludables.iterator().next().asAttribute();
        // Get the name of the attribute variable from whichever concludable is chosen
        String var = Iterators.iterate(concludables).map(Concludable::conjunction).flatMap(
                c -> Iterators.iterate(c.variables())).filter(Variable::isThing).map(v -> v.reference().syntax()).first().get();

        Rule rule = createRule("isa-rule", "{ $x isa person; }", "$x has first-name \"john\"");

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion(), conceptMgr).toList();
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put(var, set("$_0"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(0, unifier.constraintRequirements().types().size());
        assertEquals(0, unifier.constraintRequirements().isaExplicit().size());
        assertEquals(0, unifier.constraintRequirements().predicates().size());

        Rule rule2 = createRule("isa-rule-2", "{ " + var + " isa person; }", "(employee: " + var + ") isa employment");

        unifiers = queryConcludable.unify(rule2.conclusion(), conceptMgr).toList();
        assertEquals(0, unifiers.size());
    }

}
