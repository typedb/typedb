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
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.rocks.RocksDatabaseManager;
import com.vaticle.typedb.core.rocks.RocksSession;
import com.vaticle.typedb.core.rocks.RocksTransaction;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.TypeQL;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
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
import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.logic.resolvable.Util.createRule;
import static com.vaticle.typedb.core.logic.resolvable.Util.getStringMapping;
import static com.vaticle.typedb.core.logic.resolvable.Util.resolvedConjunction;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UnifyIsaConcludableTest {

    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("unify-isa-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
            .storageDataCacheSize(MB).storageIndexCacheSize(MB);
    private static final String database = "unify-isa-test";
    private static RocksDatabaseManager databaseManager;
    private static RocksSession session;
    private static RocksTransaction rocksTransaction;
    private static ConceptManager conceptMgr;
    private static LogicManager logicMgr;

    @BeforeClass
    public static void setUp() throws IOException {
        Util.resetDirectory(dataDir);
        databaseManager = RocksDatabaseManager.open(options);
        databaseManager.create(database);
        session = databaseManager.session(database, Arguments.Session.Type.SCHEMA);
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
        databaseManager.close();
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
        assert type != null;
        if (type.isEntityType()) return type.asEntityType().create();
        else if (type.isRelationType()) return type.asRelationType().create();
        else if (type.isAttributeType() && type.asAttributeType().isString())
            return type.asAttributeType().asString().put("john");
        else if (type.isAttributeType() && type.asAttributeType().isLong())
            return type.asAttributeType().asLong().put(10L);
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    private Thing instanceOf(String stringAttributeLabel, String stringValue) {
        AttributeType type = conceptMgr.getAttributeType(stringAttributeLabel);
        assert type != null;
        return type.asString().put(stringValue);
    }

    @Test
    public void isa_variable_unifies_rule_has_exact() {
        String conjunction = "{ $a isa $t; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Isa queryConcludable = concludables.iterator().next().asIsa();

        Rule rule = createRule("isa-rule", "{ $x isa person; }",
                               "$x has first-name \"john\"", logicMgr);

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion(), conceptMgr).toList();
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$a", set("$_0"));
            put("$t", set("$_first-name"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(1, unifier.requirements().types().size());
        assertEquals(1, unifier.requirements().isaExplicit().size());
        assertEquals(0, unifier.requirements().predicates().size());
    }

    @Test
    public void isa_variable_unifies_rule_relation_exact() {
        String conjunction = "{ $a isa $t; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Isa queryConcludable = concludables.iterator().next().asIsa();

        Rule rule = createRule("isa-rule", "{ $x isa person; }",
                               "(employee: $x) isa employment", logicMgr);

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion(), conceptMgr).toList();
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        HashMap<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$a", set("$_0"));
            put("$t", set("$_employment"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(1, unifier.requirements().types().size());
        assertEquals(1, unifier.requirements().isaExplicit().size());
        assertEquals(0, unifier.requirements().predicates().size());
    }

    @Test
    public void isa_variable_unifies_relation_variable_type() {
        String conjunction = "{ $a isa $t; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Isa queryConcludable = concludables.iterator().next().asIsa();

        Rule rule = createRule("isa-rule",
                               "{ $x isa person; $role-type type employment:employee; $rel-type relates $role-type; }",
                               "($role-type: $x) isa $rel-type", logicMgr);

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion(), conceptMgr).toList();
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        HashMap<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$a", set("$_0"));
            put("$t", set("$rel-type"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(1, unifier.requirements().types().size());
        assertEquals(1, unifier.requirements().isaExplicit().size());
        assertEquals(0, unifier.requirements().predicates().size());
    }

    /*
    Test unifier pruning using type information
     */
    @Test
    public void isa_variable_with_type_prunes_irrelevant_rules() {
        String conjunction = "{ $a isa $t; $t type company; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Isa queryConcludable = concludables.iterator().next().asIsa();

        Rule rule = createRule("isa-rule", "{ $x isa person; }", "$x has first-name \"john\"", logicMgr);
        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion(), conceptMgr).toList();
        assertEquals(0, unifiers.size());

        Rule rule2 = createRule("isa-rule-2", "{ $x isa person; }", "(employee: $x) isa employment", logicMgr);
        unifiers = queryConcludable.unify(rule.conclusion(), conceptMgr).toList();
        assertEquals(0, unifiers.size());

        Rule rule3 = createRule("isa-rule-3",
                                "{ $x isa person; $role-type type employment:employee; $rel-type relates $role-type; }",
                                "($role-type: $x) isa $rel-type", logicMgr);
        unifiers = queryConcludable.unify(rule.conclusion(), conceptMgr).toList();

        assertEquals(0, unifiers.size());

    }

    @Test
    public void isa_exact_unifies_rule_has_exact() {
        String conjunction = "{ $a isa name; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Isa queryConcludable = concludables.iterator().next().asIsa();

        Rule rule = createRule("isa-rule", "{ $x isa person; }",
                               "$x has first-name \"john\"", logicMgr);

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion(), conceptMgr).toList();
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$a", set("$_0"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(1, unifier.requirements().types().size());
        assertEquals(1, unifier.requirements().isaExplicit().size());
        assertEquals(set(Label.of("first-name"), Label.of("last-name")),
                     unifier.requirements().isaExplicit().values().iterator().next());
        assertEquals(0, unifier.requirements().predicates().size());

        // test filter allows a valid answer
        Map<Identifier.Variable, Concept> concepts = map(
                pair(Identifier.Variable.anon(0), instanceOf("first-name", "john")),
                pair(Identifier.Variable.label("first-name"), conceptMgr.getThingType("first-name"))
        );
        FunctionalIterator<ConceptMap> unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertTrue(unified.hasNext());
        assertEquals(1, unified.next().concepts().size());

        // filter out invalid type
        concepts = map(
                pair(Identifier.Variable.anon(0), instanceOf("age")),
                pair(Identifier.Variable.label("first-name"), conceptMgr.getThingType("age"))
        );
        unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertFalse(unified.hasNext());
    }

    @Test
    public void isa_exact_unifies_rule_relation_exact() {
        String conjunction = "{ $a isa relation; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Isa queryConcludable = concludables.iterator().next().asIsa();

        Rule rule = createRule("isa-rule", "{ $x isa person; }", "(employee: $x) isa employment", logicMgr);

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion(), conceptMgr).toList();
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        HashMap<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$a", set("$_0"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(1, unifier.requirements().types().size());
        assertEquals(1, unifier.requirements().isaExplicit().size());
        assertEquals(set(Label.of("employment")), unifier.requirements().isaExplicit().get(Identifier.Variable.name("a")));
        assertEquals(0, unifier.requirements().predicates().size());

        // test filter allows a valid answer
        Map<Identifier.Variable, Concept> concepts = map(
                pair(Identifier.Variable.anon(0), instanceOf("employment")),
                pair(Identifier.Variable.label("employment"), conceptMgr.getThingType("employment"))
        );
        FunctionalIterator<ConceptMap> unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertTrue(unified.hasNext());
        assertEquals(1, unified.next().concepts().size());

        // filter out invalid type
        concepts = map(
                pair(Identifier.Variable.anon(0), instanceOf("age")),
                pair(Identifier.Variable.label("employment"), conceptMgr.getThingType("age"))
        );
        unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertFalse(unified.hasNext());
    }

    @Test
    public void isa_exact_unifies_rule_relation_variable() {
        String conjunction = "{ $a isa relation; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Isa queryConcludable = concludables.iterator().next().asIsa();

        Rule rule = createRule("isa-rule",
                               "{ $x isa person; $role-type type employment:employee; $rel-type relates $role-type; }",
                               "($role-type: $x) isa $rel-type", logicMgr);

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion(), conceptMgr).toList();
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        HashMap<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$a", set("$_0"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(1, unifier.requirements().types().size());
        assertEquals(1, unifier.requirements().isaExplicit().size());
        assertEquals(set(Label.of("employment")), unifier.requirements().isaExplicit().get(Identifier.Variable.name("a")));
        assertEquals(0, unifier.requirements().predicates().size());

        // test filter allows a valid answer
        Map<Identifier.Variable, Concept> concepts = map(
                pair(Identifier.Variable.anon(0), instanceOf("employment")),
                pair(Identifier.Variable.name("rel-type"), conceptMgr.getThingType("employment"))
        );
        FunctionalIterator<ConceptMap> unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertTrue(unified.hasNext());
        assertEquals(1, unified.next().concepts().size());

        // filter out invalid type
        concepts = map(
                pair(Identifier.Variable.anon(0), instanceOf("age")),
                pair(Identifier.Variable.name("rel-type"), conceptMgr.getThingType("age"))
        );
        unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertFalse(unified.hasNext());
    }

    @Test
    public void isa_concrete_prunes_irrelevant_rules() {
        String conjunction = "{ $a isa age; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Isa queryConcludable = concludables.iterator().next().asIsa();

        Rule rule = createRule("isa-rule", "{ $x isa person; }",
                               "$x has first-name \"john\"", logicMgr);

        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion(), conceptMgr).toList();
        assertEquals(0, unifiers.size());

        Rule rule2 = createRule("isa-rule-2", "{ $x isa person; }",
                                "(employee: $x) isa employment", logicMgr);
        unifiers = queryConcludable.unify(rule2.conclusion(), conceptMgr).toList();
        assertEquals(0, unifiers.size());

        Rule rule3 = createRule("isa-rule-3",
                                "{ $x isa person; $role-type type employment:employee; $rel-type relates $role-type; }",
                                "($role-type: $x) isa $rel-type", logicMgr);
        unifiers = queryConcludable.unify(rule3.conclusion(), conceptMgr).toList();
        assertEquals(0, unifiers.size());
    }


    @Ignore // TODO: enable when we have query rewriting to handle all predicates at once
    @Test
    public void isa_with_attribute_value() {
        String conjunction = "{ $a 'john' isa name; }";
        // TODO: this should be treated is `$a isa name; $a = "john"` until we have query rewriting
    }

    @Ignore // TODO: enable when we have query rewriting to handle all predicates at once
    @Test
    public void isa_with_attribute_literal_predicates() {
        String conjunction = "{ $a isa age; $a > 0; $a < 10; }";
        // TODO
    }

    @Ignore // TODO: enable when we have query rewriting to handle all predicates at once
    @Test
    public void isa_predicates_can_filter_answers() {
        String conjunction = "{ $a isa first-name; $a > 'b'; $a < 'y'; $a contains 'j'; }";
        Set<Concludable> concludables = Concludable.create(resolvedConjunction(conjunction, logicMgr));
        Concludable.Isa queryConcludable = concludables.iterator().next().asIsa();

        Rule rule = createRule("isa-rule", "{ $x isa person; }", "$x has first-name \"john\"", logicMgr);
        List<Unifier> unifiers = queryConcludable.unify(rule.conclusion(), conceptMgr).toList();
        assertEquals(1, unifiers.size());

        Unifier unifier = unifiers.get(0);

        // test requirements
        assertEquals(0, unifier.requirements().types().size());
        assertEquals(1, unifier.requirements().isaExplicit().size());
        assertEquals(3, unifier.requirements().predicates().size());

        // test filter allows a valid answer
        assertEquals(set(Label.of("first-name")), unifier.requirements().isaExplicit().get(Identifier.Variable.name("a")));
        Map<Identifier.Variable, Concept> concepts = map(
                pair(Identifier.Variable.name("x"), instanceOf("first-name", "johnny"))
        );
        FunctionalIterator<ConceptMap> unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertTrue(unified.hasNext());
        assertEquals(1, unified.next().concepts().size());

        // filter out using >
        concepts = map(
                pair(Identifier.Variable.name("x"), instanceOf("first-name", "abe"))
        );
        unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertFalse(unified.hasNext());

        // filter out using <
        concepts = map(
                pair(Identifier.Variable.name("x"), instanceOf("first-name", "zack"))
        );
        unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertFalse(unified.hasNext());

        // filter out using contains
        concepts = map(
                pair(Identifier.Variable.name("x"), instanceOf("first-name", "carol"))
        );
        unified = unifier.unUnify(concepts, new Unifier.Requirements.Instance(map()));
        assertFalse(unified.hasNext());

    }
}
