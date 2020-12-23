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
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.set;
import static grakn.core.pattern.variable.VariableRegistry.createFromThings;
import static org.junit.Assert.assertEquals;

public class UnifyIsaConcludableTest {

    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("unification-test");
    private static String database = "unification-test";
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


    // ############## Isa Unification ###############

    @Test
    public void isa_variable_unifies() {
        ConceptManager conceptMgr = rocksTransaction.concepts();

        String conjunction = "{ $a isa $t; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Isa queryConcludable = concludables.iterator().next().asIsa();

        // rule: when { $x isa person; } then { $x has name "john"; }
        Conjunction whenHasName = parseConjunction("{$x isa person;}");
        Conjunction thenHasNameJohn = parseConjunction("{ $x has name 'john'; }");
        IsaConstraint thenHasNameIsa = findIsaConstraint(thenHasNameJohn);
        Rule.Conclusion.Isa hasIsaConclusion = Rule.Conclusion.Isa.create(thenHasNameIsa, whenHasName.variables());

        List<Unifier> unifiers = queryConcludable.unify(hasIsaConclusion, conceptMgr).collect(Collectors.toList());
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$a", set("$_0"));
            put("$t", set("$_name"));
        }};
        assertEquals(expected, result);

        // rule: when { $x isa person; } then { (employee: $x) isa employment; }
        Conjunction whenExactRelation = parseConjunction("{ $x isa person; }");
        Conjunction thenExactRelation = parseConjunction("{ (employee: $x) isa employment; }");
        IsaConstraint thenEmploymentIsa = findIsaConstraint(thenExactRelation);
        Rule.Conclusion.Isa relationIsaExactConclusion = Rule.Conclusion.Isa.create(thenEmploymentIsa, whenExactRelation.variables());

        unifiers = queryConcludable.unify(relationIsaExactConclusion, conceptMgr).collect(Collectors.toList());
        assertEquals(1, unifiers.size());
        unifier = unifiers.get(0);
        result = getStringMapping(unifier.mapping());
        expected = new HashMap<String, Set<String>>() {{
            put("$a", set("$_0"));
            put("$t", set("$_employment"));
        }};
        assertEquals(expected, result);

        // rule: when { $x isa person; $role-type type employment:employee; $rel-type relates $role-type;} then { ($role-type: $x) isa $rel-type; }
        Conjunction whenVariableRelType = parseConjunction("{ $x isa person; $role-type type employment:employee; $rel-type relates $role-type; }");
        Conjunction thenVariableRelType = parseConjunction("{ ($role-type: $x) isa $rel-type; }");
        IsaConstraint thenVariableRelTypeIsa = findIsaConstraint(thenVariableRelType);
        Rule.Conclusion.Isa relationIsaVariableRelType = Rule.Conclusion.Isa.create(thenVariableRelTypeIsa, whenVariableRelType.variables());

        unifiers = queryConcludable.unify(relationIsaVariableRelType, conceptMgr).collect(Collectors.toList());
        assertEquals(1, unifiers.size());
        unifier = unifiers.get(0);
        result = getStringMapping(unifier.mapping());
        expected = new HashMap<String, Set<String>>() {{
            put("$a", set("$_0"));
            put("$t", set("$rel-type"));
        }};
        assertEquals(expected, result);
    }

    @Test
    public void isa_variable_fails_pruning() {
        ConceptManager conceptMgr = rocksTransaction.concepts();

        String conjunction = "{ $a isa $t; $t type company; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Isa queryConcludable = concludables.iterator().next().asIsa();

        // rule: when { $x isa person; } then { $x has name "john"; }
        Conjunction whenHasName = parseConjunction("{$x isa person;}");
        Conjunction thenHasNameJohn = parseConjunction("{ $x has name 'john'; }");
        IsaConstraint thenHasNameIsa = findIsaConstraint(thenHasNameJohn);
        Rule.Conclusion.Isa hasIsaConclusion = Rule.Conclusion.Isa.create(thenHasNameIsa, whenHasName.variables());
        List<Unifier> unifiers = queryConcludable.unify(hasIsaConclusion, conceptMgr).collect(Collectors.toList());
        assertEquals(0, unifiers.size());

        // rule: when { $x isa person; } then { (employee: $x) isa employment; }
        Conjunction whenExactRelation = parseConjunction("{ $x isa person; }");
        Conjunction thenExactRelation = parseConjunction("{ (employee: $x) isa employment; }");
        IsaConstraint thenEmploymentIsa = findIsaConstraint(thenExactRelation);
        Rule.Conclusion.Isa relationIsaExactConclusion = Rule.Conclusion.Isa.create(thenEmploymentIsa, whenExactRelation.variables());
        unifiers = queryConcludable.unify(relationIsaExactConclusion, conceptMgr).collect(Collectors.toList());
        assertEquals(0, unifiers.size());

        // rule: when { $x isa person; $role-type type employment:employee; $rel-type relates $role-type;} then { ($role-type: $x) isa $rel-type; }
        Conjunction whenVariableRelType = parseConjunction("{ $x isa person; $role-type type employment:employee; $rel-type relates $role-type; }");
        Conjunction thenVariableRelType = parseConjunction("{ ($role-type: $x) isa $rel-type; }");
        IsaConstraint thenVariableRelTypeIsa = findIsaConstraint(thenVariableRelType);
        Rule.Conclusion.Isa relationIsaVariableRelType = Rule.Conclusion.Isa.create(thenVariableRelTypeIsa, whenVariableRelType.variables());
        unifiers = queryConcludable.unify(relationIsaVariableRelType, conceptMgr).collect(Collectors.toList());
        assertEquals(0, unifiers.size());
    }


    @Test
    public void isa_concrete_unifies() {
        ConceptManager conceptMgr = rocksTransaction.concepts();

        String conjunction = "{ $a isa name; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Isa queryConcludable = concludables.iterator().next().asIsa();

        // rule: when { $x isa person; } then { $x has name "john"; }
        Conjunction whenHasName = parseConjunction("{$x isa person;}");
        Conjunction thenHasNameJohn = parseConjunction("{ $x has name 'john'; }");
        IsaConstraint thenHasNameIsa = findIsaConstraint(thenHasNameJohn);
        Rule.Conclusion.Isa hasIsaConclusion = Rule.Conclusion.Isa.create(thenHasNameIsa, whenHasName.variables());

        List<Unifier> unifiers = queryConcludable.unify(hasIsaConclusion, conceptMgr).collect(Collectors.toList());
        assertEquals(1, unifiers.size());
        Unifier unifier = unifiers.get(0);
        Map<String, Set<String>> result = getStringMapping(unifier.mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$a", set("$_0"));
            put("$_name", set("$_name"));
        }};
        assertEquals(expected, result);


        conjunction = "{ $a isa employment; }";
        concludables = Concludable.create(parseConjunction(conjunction));
        queryConcludable = concludables.iterator().next().asIsa();

        // rule: when { $x isa person; } then { (employee: $x) isa employment; }
        Conjunction whenExactRelation = parseConjunction("{ $x isa person; }");
        Conjunction thenExactRelation = parseConjunction("{ (employee: $x) isa employment; }");
        IsaConstraint thenEmploymentIsa = findIsaConstraint(thenExactRelation);
        Rule.Conclusion.Isa relationIsaExactConclusion = Rule.Conclusion.Isa.create(thenEmploymentIsa, whenExactRelation.variables());

        unifiers = queryConcludable.unify(relationIsaExactConclusion, conceptMgr).collect(Collectors.toList());
        assertEquals(1, unifiers.size());
        unifier = unifiers.get(0);
        result = getStringMapping(unifier.mapping());
        expected = new HashMap<String, Set<String>>() {{
            put("$a", set("$_0"));
            put("$_employment", set("$_employment"));
        }};
        assertEquals(expected, result);

        // rule: when { $x isa person; $role-type type employment:employee; $rel-type relates $role-type;} then { ($role-type: $x) isa $rel-type; }
        Conjunction whenVariableRelType = parseConjunction("{ $x isa person; $role-type type employment:employee; $rel-type relates $role-type; }");
        Conjunction thenVariableRelType = parseConjunction("{ ($role-type: $x) isa $rel-type; }");
        IsaConstraint thenVariableRelTypeIsa = findIsaConstraint(thenVariableRelType);
        Rule.Conclusion.Isa relationIsaVariableRelType = Rule.Conclusion.Isa.create(thenVariableRelTypeIsa, whenVariableRelType.variables());

        unifiers = queryConcludable.unify(relationIsaVariableRelType, conceptMgr).collect(Collectors.toList());
        assertEquals(1, unifiers.size());
        unifier = unifiers.get(0);
        result = getStringMapping(unifier.mapping());
        expected = new HashMap<String, Set<String>>() {{
            put("$a", set("$_0"));
            put("$_employment", set("$rel-type"));
        }};
        assertEquals(expected, result);
    }


    @Test
    public void isa_concrete_fails_pruning() {
        ConceptManager conceptMgr = rocksTransaction.concepts();

        String conjunction = "{ $a isa age; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Isa queryConcludable = concludables.iterator().next().asIsa();

        // rule: when { $x isa person; } then { $x has name "john"; }
        Conjunction whenHasName = parseConjunction("{$x isa person;}");
        Conjunction thenHasNameJohn = parseConjunction("{ $x has name 'john'; }");
        IsaConstraint thenHasNameIsa = findIsaConstraint(thenHasNameJohn);
        Rule.Conclusion.Isa hasIsaConclusion = Rule.Conclusion.Isa.create(thenHasNameIsa, whenHasName.variables());

        List<Unifier> unifiers = queryConcludable.unify(hasIsaConclusion, conceptMgr).collect(Collectors.toList());
        assertEquals(0, unifiers.size());

        // rule: when { $x isa person; } then { (employee: $x) isa employment; }
        Conjunction whenExactRelation = parseConjunction("{ $x isa person; }");
        Conjunction thenExactRelation = parseConjunction("{ (employee: $x) isa employment; }");
        IsaConstraint thenEmploymentIsa = findIsaConstraint(thenExactRelation);
        Rule.Conclusion.Isa relationIsaExactConclusion = Rule.Conclusion.Isa.create(thenEmploymentIsa, whenExactRelation.variables());

        unifiers = queryConcludable.unify(relationIsaExactConclusion, conceptMgr).collect(Collectors.toList());
        assertEquals(0, unifiers.size());

        // rule: when { $x isa person; $role-type type employment:employee; $rel-type relates $role-type;} then { ($role-type: $x) isa $rel-type; }
        Conjunction whenVariableRelType = parseConjunction("{ $x isa person; $role-type type employment:employee; $rel-type relates $role-type; }");
        Conjunction thenVariableRelType = parseConjunction("{ ($role-type: $x) isa $rel-type; }");
        IsaConstraint thenVariableRelTypeIsa = findIsaConstraint(thenVariableRelType);
        Rule.Conclusion.Isa relationIsaVariableRelType = Rule.Conclusion.Isa.create(thenVariableRelTypeIsa, whenVariableRelType.variables());

        unifiers = queryConcludable.unify(relationIsaVariableRelType, conceptMgr).collect(Collectors.toList());
        assertEquals(0, unifiers.size());
    }
}
