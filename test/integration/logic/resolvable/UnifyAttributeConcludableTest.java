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
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class UnifyAttributeConcludableTest {

    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("unify-attribute-test");
    private static String database = "unify-attribute-test";
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

    @Test
    public void variable_with_predicates_unifies_only_isa_rules() {
        String conjunction = "{ $x < 7; $x > 0; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Value queryConcludable = concludables.iterator().next().asValue();

        /*
        TODO build attribute concludable out of the conjunction
        This should successfully unify with rules that can conclude new concepts
        and nothing else

        The unifier should also contain the predicates, and reject answers not
        satisfying the predicates
         */

//        Conjunction thenConjunction = parseConjunction("{ $a > $num; $a isa $person; $num = 7; }");
//        ThingVariable variable = parseThingVariable("$a > $num", "a");
//        ValueConstraint<?> valueConstraint = variable.value().iterator().next();
//        Rule.Conclusion.Value valueConcludable = Rule.Conclusion.Value.create(valueConstraint, thenConjunction.variables());
//
//        Optional<Unifier> unifier = conjConcludable.unify(valueConcludable, conceptMgr).findFirst();
//        assertTrue(unifier.isPresent());
//        Map<String, Set<String>> result = getStringMapping(unifier.get().mapping());
//        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
//            put("$x", set("$a"));
//        }};
//        assertTrue(result.entrySet().containsAll(expected.entrySet()));
//        assertEquals(expected, result);
    }

    @Test
    public void variable_with_variable_predicate_unifies_isa_rules() {
        String conjunction = "{ $x > $y; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Value queryConcludable = concludables.iterator().next().asValue();

        /*
        TODO build attribute concludables (2) out of the conjunction
        Each should successfully unify with rules that can conclude new concepts
        and nothing else
        */

//        Conjunction thenConjunction = parseConjunction("{ $a > $num; $a isa $person; $num = 7; }");
//        ThingVariable variable = parseThingVariable("$a > $num", "a");
//        ValueConstraint<?> valueConstraint = variable.value().iterator().next();
//        Rule.Conclusion.Value valueConcludable = Rule.Conclusion.Value.create(valueConstraint, thenConjunction.variables());
//
//        Optional<Unifier> unifier = conjConcludable.unify(valueConcludable, conceptMgr).findFirst();
//        assertTrue(unifier.isPresent());
//        Map<String, Set<String>> result = getStringMapping(unifier.get().mapping());
//        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
//            put("$x", set("$a"));
//            put("$y", set("$num"));
//        }};
//        assertTrue(result.entrySet().containsAll(expected.entrySet()));
//        assertEquals(expected, result);
    }

}
