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
import grakn.core.concept.ConceptManager;
import grakn.core.logic.Rule;
import grakn.core.logic.transformer.Unifier;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import grakn.core.test.integration.util.Util;
import grakn.core.traversal.common.Identifier;
import graql.lang.Graql;
import graql.lang.pattern.variable.Reference;
import org.junit.After;
import org.junit.Before;
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

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.set;
import static grakn.core.pattern.variable.VariableRegistry.createFromThings;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UnifyHasConcludableTest {

    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("unify-has-test");
    private static String database = "unify-has-test";
    private static RocksGrakn grakn;
    private static RocksSession session;
    private static RocksTransaction rocksTransaction;

    @Before
    public void setUp() throws IOException {
        Util.resetDirectory(directory);
        grakn = RocksGrakn.open(directory);
        grakn.databases().create(database);
        session = grakn.session(database, Arguments.Session.Type.SCHEMA);
        try (RocksTransaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
            tx.query().define(Graql.parseQuery("define " +
                                                       "person sub entity," +
                                                       "    owns name," +
                                                       "    owns age," +
                                                       "    plays employment:employee;" +
                                                       "company sub entity," +
                                                       "    plays employment:employer;" +
                                                       "employment sub relation," +
                                                       "    relates employee," +
                                                       "    relates employer;" +
                                                       "name sub attribute, value string;" +
                                                       "age sub attribute, value long;" +
                                                       "").asDefine());
            tx.commit();
        }
        rocksTransaction = session.transaction(Arguments.Transaction.Type.READ);
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

    private Conjunction parseConjunction(String query) {
        return Disjunction.create(Graql.parsePattern(query).asConjunction().normalise()).conjunctions().iterator().next();
    }

    private ThingVariable parseThingVariable(String graqlVariable, String variableName) {
        return createFromThings(list(Graql.parseVariable(graqlVariable).asThing())).get(Reference.Name.named(variableName)).asThing();
    }

    private IsaConstraint findIsaConstraint(Conjunction conjunction) {
        List<IsaConstraint> isas = conjunction.variables().stream().flatMap(var -> var.constraints().stream())
                .filter(constraint -> constraint.isThing() && constraint.asThing().isIsa())
                .map(constraint -> constraint.asThing().asIsa()).collect(Collectors.toList());
        assert isas.size() == 1 : "More than 1 isa constraint in conjunction to search";
        return isas.get(0);
    }

    //TODO: create more tests when type inference is working. (That is why this is an integration test).

//

//    @Test
//    public void unify_has_concrete() {
//        String conjunction = "{ $x has name 'bob'; }";
//        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
//        Concludable.Has conjConcludable = concludables.iterator().next().asHas();
//
//        Conjunction thenConjunction = parseConjunction("{ $p isa $person; $p has $name; $name = 'bob' isa name;}");
//        ThingVariable variable = parseThingVariable("$p has $name", "p");
//        HasConstraint hasConstraint = variable.has().iterator().next();
//        Rule.Conclusion.Has hasConclusion = new Rule.Conclusion.Has(hasConstraint, thenConjunction.variables());
//
//        Optional<Unifier> unifier = conjConcludable.unify(hasConclusion, conceptMgr).findFirst();
//        assertTrue(unifier.isPresent());
//        Map<String, Set<String>> result = getStringMapping(unifier.get().mapping());
//        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
//            put("$x", set("$p"));
//        }};
//        assertTrue(result.entrySet().containsAll(expected.entrySet()));
//        assertEquals(expected, result);
//    }
//
//    @Test
//    public void unify_has_variable() {
//        String conjunction = "{ $x has $y; }";
//        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
//        Concludable.Has conjConcludable = concludables.iterator().next().asHas();
//
//        Conjunction thenConjunction = parseConjunction("{ $p isa $person; $p has $name; $name = 'bob' isa name;}");
//        ThingVariable variable = parseThingVariable("$p has $name", "p");
//        HasConstraint hasConstraint = variable.has().iterator().next();
//        Rule.Conclusion.Has hasConcludable = new Rule.Conclusion.Has(hasConstraint, thenConjunction.variables());
//
//        Optional<Unifier> unifier = conjConcludable.unify(hasConcludable, conceptMgr).findFirst();
//        assertTrue(unifier.isPresent());
//        Map<String, Set<String>> result = getStringMapping(unifier.get().mapping());
//        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
//            put("$x", set("$p"));
//            put("$y", set("$name"));
//        }};
//        assertTrue(result.entrySet().containsAll(expected.entrySet()));
//        assertEquals(expected, result);
//    }
//
//    @Test
//    public void unify_has_syntax_sugar() {
//        String conjunction = "{ $x has name $y; }";
//        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
//        Concludable.Has conjConcludable = concludables.iterator().next().asHas();
//
//        Conjunction thenConjunction = parseConjunction("{ $p isa $person; $p has $name; $name = 'bob' isa name;}");
//        ThingVariable variable = parseThingVariable("$p has $name", "p");
//        HasConstraint hasConstraint = variable.has().iterator().next();
//        Rule.Conclusion.Has hasConcludable = new Rule.Conclusion.Has(hasConstraint, thenConjunction.variables());
//
//        Optional<Unifier> unifier = conjConcludable.unify(hasConcludable, conceptMgr).findFirst();
//        assertTrue(unifier.isPresent());
//        Map<String, Set<String>> result = getStringMapping(unifier.get().mapping());
//        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
//            put("$x", set("$p"));
//            put("$y", set("$name"));
//        }};
//        assertTrue(result.entrySet().containsAll(expected.entrySet()));
//        assertEquals(expected, result);
//    }

//    @Test
//    public void has_duplicate_vars_then() {
//        String conjunction = "{ $x has name $y; }";
//        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
//        Concludable.Has conjConcludable = concludables.iterator().next().asHas();
//
//        Conjunction thenConjunction = parseConjunction("{ $a has $a;}");
//        ThingVariable variable = parseThingVariable("$a has $a", "a");
//        HasConstraint hasConstraint = variable.has().iterator().next();
//        Rule.Conclusion.Has hasConcludable = new Rule.Conclusion.Has(hasConstraint, thenConjunction.variables());
//
//        Optional<Unifier> unifier = conjConcludable.unify(hasConcludable, conceptMgr).findFirst();
//        assertTrue(unifier.isPresent());
//        Map<String, Set<String>> result = getStringMapping(unifier.get().mapping());
//        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
//            put("$x", set("$a"));
//            put("$y", set("$a"));
//        }};
//        assertTrue(result.entrySet().containsAll(expected.entrySet()));
//        assertEquals(expected, result);
//    }
//
//    @Test
//    public void has_duplicate_vars_both() {
//        String conjunction = "{ $x has name $x; }";
//        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
//        Concludable.Has conjConcludable = concludables.iterator().next().asHas();
//
//        Conjunction thenConjunction = parseConjunction("{ $a has $a;}");
//        ThingVariable variable = parseThingVariable("$a has $a", "a");
//        HasConstraint hasConstraint = variable.has().iterator().next();
//        Rule.Conclusion.Has hasConcludable = new Rule.Conclusion.Has(hasConstraint, thenConjunction.variables());
//
//        Optional<Unifier> unifier = conjConcludable.unify(hasConcludable, conceptMgr).findFirst();
//        assertTrue(unifier.isPresent());
//        Map<String, Set<String>> result = getStringMapping(unifier.get().mapping());
//        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
//            put("$x", set("$a"));
//        }};
//        assertTrue(result.entrySet().containsAll(expected.entrySet()));
//        assertEquals(expected, result);
//    }

}
