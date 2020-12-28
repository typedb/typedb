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

import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Label;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.AttributeType;
import grakn.core.logic.LogicManager;
import grakn.core.logic.Rule;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.pattern.constraint.thing.IsaConstraint;
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

    private IsaConstraint findIsaConstraint(Conjunction conjunction) {
        List<IsaConstraint> isas = conjunction.variables().stream().flatMap(var -> var.constraints().stream())
                .filter(constraint -> constraint.isThing() && constraint.asThing().isIsa())
                .map(constraint -> constraint.asThing().asIsa()).collect(Collectors.toList());
        assert isas.size() == 1 : "More than 1 isa constraint in conjunction to search";
        return isas.get(0);
    }

    @Ignore // TODO enable when we have the final structure of conclusions and concludable in place
    @Test
    public void literal_predicates_unify_and_filter_answers() {
        String conjunction = "{ $a > 'b'; $a < 'y'; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Value queryConcludable = concludables.iterator().next().asValue();

        // rule: when { $x isa person; } then { $x has first-name "john"; }
        Conjunction whenHasName = parseConjunction("{$x isa person;}");
        Conjunction thenHasNameJohn = parseConjunction("{ $x has first-name 'john'; }");
        IsaConstraint thenHasNameIsa = findIsaConstraint(thenHasNameJohn);
        Rule.Conclusion.Isa hasIsaConclusion = Rule.Conclusion.Isa.create(thenHasNameIsa, whenHasName.variables()); // TODO this will become a Has conclusion

        List<Unifier> unifiers = queryConcludable.unify(hasIsaConclusion, conceptMgr).collect(Collectors.toList());
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$a", set("$_0"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(0, unifier.requirements().types().size());
        assertEquals(0, unifier.requirements().isaExplicit().size());
        assertEquals(2, unifier.requirements().predicates().size());

        // test filter allows a valid answer
        Map<Identifier, Concept> identifiedConcepts = map(
                pair(Identifier.Variable.name("x"), instanceOf("first-name", "johnny"))
        );
        Optional<ConceptMap> unified = unifier.unUnify(identifiedConcepts);
        assertTrue(unified.isPresent());
        assertEquals(1, unified.get().concepts().size());

        // filter out using >
        identifiedConcepts = map(
                pair(Identifier.Variable.name("x"), instanceOf("first-name", "abe"))
        );
        unified = unifier.unUnify(identifiedConcepts);
        assertFalse(unified.isPresent());

        // filter out using <
        identifiedConcepts = map(
                pair(Identifier.Variable.name("x"), instanceOf("first-name", "zack"))
        );
        unified = unifier.unUnify(identifiedConcepts);
        assertFalse(unified.isPresent());

    }

    @Ignore // TODO enable when new structure of concludables is enforced
    @Test
    public void variable_predicates_unify_value_conclusions() {
        String conjunction = "{ $x > $y; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(2, concludables.size());
        Concludable.Value queryConcludable = concludables.iterator().next().asValue();

        /*
        TODO build attribute concludables (2) out of the conjunction
        Each should successfully unify with rules that can conclude new concepts
        and nothing else
        */

        // rule: when { $x isa person; } then { $x has first-name "john"; }
        Conjunction whenHasName = parseConjunction("{$x isa person;}");
        Conjunction thenHasNameJohn = parseConjunction("{ $x has first-name 'john'; }");
        IsaConstraint thenHasNameIsa = findIsaConstraint(thenHasNameJohn);
        Rule.Conclusion.Isa hasIsaConclusion = Rule.Conclusion.Isa.create(thenHasNameIsa, whenHasName.variables()); // TODO refactor in new structure

        List<Unifier> unifiers = queryConcludable.unify(hasIsaConclusion, conceptMgr).collect(Collectors.toList());
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$a", set("$_0"));
        }};
        assertEquals(expected, result);

        // test requirements
        assertEquals(0, unifier.requirements().types().size());
        assertEquals(0, unifier.requirements().isaExplicit().size());
        assertEquals(0, unifier.requirements().predicates().size());

        // rule: when { $x isa person; } then { (employee: $x) isa employment; }
        Conjunction whenExactRelation = parseConjunction("{ $x isa person; }");
        Conjunction thenExactRelation = parseConjunction("{ (employee: $x) isa employment; }");
        IsaConstraint thenEmploymentIsa = findIsaConstraint(thenExactRelation);
        Rule.Conclusion.Isa relationIsaExactConclusion = Rule.Conclusion.Isa.create(thenEmploymentIsa, whenExactRelation.variables());

        unifiers = queryConcludable.unify(relationIsaExactConclusion, conceptMgr).collect(Collectors.toList());
        assertEquals(0, unifiers.size());
    }

}
