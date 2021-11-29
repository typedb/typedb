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

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options.Database;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.pattern.variable.Variable;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.map;
import static com.vaticle.typedb.common.collection.Collections.pair;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.logic.resolvable.Util.createRule;
import static com.vaticle.typedb.core.logic.resolvable.Util.getStringMapping;
import static com.vaticle.typedb.core.logic.resolvable.Util.resolvedConjunction;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UnifyAttributeConcludableTest {

    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("unify-attribute-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Database options = new Database().dataDir(dataDir).reasonerDebuggerDir(logDir);
    private static final String database = "unify-attribute-test";
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
        typedb.close();
    }

    @Before
    public void setUpTransaction() {
        rocksTransaction = session.transaction(Arguments.Transaction.Type.WRITE);
        conceptMgr = rocksTransaction.concepts();
        logicMgr = rocksTransaction.logic();
    }

    @After
    public void tearDownTransaction() { rocksTransaction.close(); }

    private Thing instanceOf(String stringAttributeLabel, String stringValue) {
        AttributeType type = conceptMgr.getAttributeType(stringAttributeLabel);
        assert type != null;
        return type.asString().put(stringValue);
    }

    @Test
    public void literal_predicates_unify_and_filter_answers() {
        String conjunction = "{ $a = 'john'; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Attribute queryConcludable = concludables.iterator().next().asAttribute();

        Rule rule = createRule("isa-rule", "{ $x isa person; }", "$x has first-name \"john\"", logicMgr);

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion(), conceptMgr).toList();
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$a", set("$_0"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(0, unifier.requirements().types().size());
        assertEquals(1, unifier.requirements().isaExplicit().size());
        assertEquals(1, unifier.requirements().predicates().size());

        // test forward unification can reject an invalid partial answer
        ConceptMap unUnified = new ConceptMap(map(pair(Identifier.Variable.name("a"), instanceOf("first-name", "bob"))));
        assertFalse(unifier.unify(unUnified).isPresent());

        // test filter allows a valid answer
        Map<Identifier.Variable, Concept> concepts = map(
                pair(Identifier.Variable.anon(0), instanceOf("first-name", "john"))
        );
        FunctionalIterator<ConceptMap> unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertTrue(unified.hasNext());
        assertEquals(1, unified.next().concepts().size());

        concepts = map(
                pair(Identifier.Variable.anon(0), instanceOf("first-name", "abe"))
        );
        unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertFalse(unified.hasNext());

    }

    @Test
    public void variable_predicates_unify_value_conclusions() {
        String conjunction = "{ $x > $y; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        assertEquals(2, concludables.size());
        Concludable.Attribute queryConcludable = concludables.iterator().next().asAttribute();
        // Get the name of the attribute variable from whichever concludable is chosen
        String var = Iterators.iterate(concludables).map(Concludable::pattern).flatMap(
                c -> Iterators.iterate(c.variables())).filter(Variable::isThing).map(v -> v.reference().syntax()).first().get();

        Rule rule = createRule("isa-rule", "{ $x isa person; }", "$x has first-name \"john\"", logicMgr);

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion(), conceptMgr).toList();
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put(var, set("$_0"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(0, unifier.requirements().types().size());
        assertEquals(1, unifier.requirements().isaExplicit().size());
        assertEquals(0, unifier.requirements().predicates().size());

        Rule rule2 = createRule("isa-rule-2", "{ " + var + " isa person; }", "(employee: " + var + ") isa employment", logicMgr);

        unifiers = queryConcludable.unify(rule2.conclusion(), conceptMgr).toList();
        assertEquals(0, unifiers.size());
    }

}
