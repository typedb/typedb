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

package grakn.core.logic.concludable;

import grakn.core.logic.Rule;
import grakn.core.logic.transformer.Unifier;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.variable.ThingVariable;
import graql.lang.Graql;
import graql.lang.pattern.variable.Reference;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.set;
import static grakn.core.pattern.variable.VariableRegistry.createFromThings;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UnificationTest {

    private Map<String, Set<String>> getStringMapping(Map<Reference.Name, Set<Reference.Name>> map) {
        return map.entrySet().stream().collect(Collectors.toMap(v -> v.getKey().syntax(),
                e -> e.getValue().stream().map(Reference.Name::syntax).collect(Collectors.toSet()))
        );
    }

    private Conjunction parseConjunction(String query) {
        return Disjunction.create(Graql.parsePattern(query).asConjunction().normalise()).conjunctions().iterator().next();
    }

    private ThingVariable parseThingVariable(String graqlVariable, String variableName) {
        return createFromThings(list(Graql.parseVariable(graqlVariable).asThing())).get(Reference.Name.named(variableName)).asThing();
    }

    //TODO: create more tests when type inference is working. (That is why this is an integration test).
    @Test
    public void unify_isa_variable() {
        String conjunction = "{ $x isa $y; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Isa conjConcludable = concludables.iterator().next().asIsa();

        Conjunction thenConjunction = parseConjunction("{ $a 7; $a isa $age; }");
        ThingVariable variable = parseThingVariable("$a isa $age", "a");
        assertTrue(variable.isa().isPresent());
        IsaConstraint isaConstraint = variable.isa().get();
        Rule.Conclusion.Isa isaConcludable = new Rule.Conclusion.Isa(isaConstraint, thenConjunction.variables());

        Optional<Unifier> unifier = conjConcludable.unify(isaConcludable).findFirst();
        assertTrue(unifier.isPresent());
        Map<String, Set<String>> result = getStringMapping(unifier.get().mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$x", set("$a"));
            put("$y", set("$age"));
        }};

        assertTrue(result.entrySet().containsAll(expected.entrySet()));
        assertEquals(expected, result);
    }

    @Test
    public void unify_isa_concrete() {
        String conjunction = "{ $x isa person; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Isa conjConcludable = concludables.iterator().next().asIsa();

        Conjunction thenConjunction = parseConjunction("{ $a 7; $a isa $person; }");
        ThingVariable variable = parseThingVariable("$a isa $person", "a");
        assertTrue(variable.isa().isPresent());
        IsaConstraint isaConstraint = variable.isa().get();
        Rule.Conclusion.Isa isaConcludable = new Rule.Conclusion.Isa(isaConstraint, thenConjunction.variables());

        Optional<Unifier> unifier = conjConcludable.unify(isaConcludable).findFirst();
        assertTrue(unifier.isPresent());
        Map<String, Set<String>> result = getStringMapping(unifier.get().mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$x", set("$a"));
        }};
        assertTrue(result.entrySet().containsAll(expected.entrySet()));
        assertEquals(expected, result);
    }

    @Test
    public void unify_value_concrete() {
        String conjunction = "{ $x = 7; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Value conjConcludable = concludables.iterator().next().asValue();

        Conjunction thenConjunction = parseConjunction("{ $a = $b; $a isa $person; $num = 7; }");
        ThingVariable variable = parseThingVariable("$a = 7", "a");
        ValueConstraint<?> valueConstraint = variable.value().iterator().next();
        Rule.Conclusion.Value valueConcludable = Rule.Conclusion.Value.create(valueConstraint, thenConjunction.variables());

        Optional<Unifier> unifier = conjConcludable.unify(valueConcludable).findFirst();
        assertTrue(unifier.isPresent());
        Map<String, Set<String>> result = getStringMapping(unifier.get().mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$x", set("$a"));
        }};
        assertTrue(result.entrySet().containsAll(expected.entrySet()));
        assertEquals(expected, result);
    }

    @Test
    public void unify_value_predicate() {
        String conjunction = "{ $x > 7; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Value conjConcludable = concludables.iterator().next().asValue();

        Conjunction thenConjunction = parseConjunction("{ $a > $num; $a isa $person; $num = 7; }");
        ThingVariable variable = parseThingVariable("$a > $num", "a");
        ValueConstraint<?> valueConstraint = variable.value().iterator().next();
        Rule.Conclusion.Value valueConcludable = Rule.Conclusion.Value.create(valueConstraint, thenConjunction.variables());

        Optional<Unifier> unifier = conjConcludable.unify(valueConcludable).findFirst();
        assertTrue(unifier.isPresent());
        Map<String, Set<String>> result = getStringMapping(unifier.get().mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$x", set("$a"));
        }};
        assertTrue(result.entrySet().containsAll(expected.entrySet()));
        assertEquals(expected, result);
    }

    @Test
    public void unify_value_variable() {
        String conjunction = "{ $x > $y; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Value conjConcludable = concludables.iterator().next().asValue();

        Conjunction thenConjunction = parseConjunction("{ $a > $num; $a isa $person; $num = 7; }");
        ThingVariable variable = parseThingVariable("$a > $num", "a");
        ValueConstraint<?> valueConstraint = variable.value().iterator().next();
        Rule.Conclusion.Value valueConcludable = Rule.Conclusion.Value.create(valueConstraint, thenConjunction.variables());

        Optional<Unifier> unifier = conjConcludable.unify(valueConcludable).findFirst();
        assertTrue(unifier.isPresent());
        Map<String, Set<String>> result = getStringMapping(unifier.get().mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$x", set("$a"));
            put("$y", set("$num"));
        }};
        assertTrue(result.entrySet().containsAll(expected.entrySet()));
        assertEquals(expected, result);
    }

    @Test
    public void unify_has_concrete() {
        String conjunction = "{ $x has name 'bob'; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Has conjConcludable = concludables.iterator().next().asHas();

        Conjunction thenConjunction = parseConjunction(
                "{ $p isa $person; $p has $name; $name = 'bob' isa name;}");
        ThingVariable variable = parseThingVariable("$p has $name", "p");
        HasConstraint hasConstraint = variable.has().iterator().next();
        Rule.Conclusion.Has hasConcludable = new Rule.Conclusion.Has(hasConstraint, thenConjunction.variables());

        Optional<Unifier> unifier = conjConcludable.unify(hasConcludable).findFirst();
        assertTrue(unifier.isPresent());
        Map<String, Set<String>> result = getStringMapping(unifier.get().mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$x", set("$p"));
        }};
        assertTrue(result.entrySet().containsAll(expected.entrySet()));
        assertEquals(expected, result);
    }

    @Test
    public void unify_has_variable() {
        String conjunction = "{ $x has $y; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Has conjConcludable = concludables.iterator().next().asHas();

        Conjunction thenConjunction = parseConjunction(
                "{ $p isa $person; $p has $name; $name = 'bob' isa name;}");
        ThingVariable variable = parseThingVariable("$p has $name", "p");
        HasConstraint hasConstraint = variable.has().iterator().next();
        Rule.Conclusion.Has hasConcludable = new Rule.Conclusion.Has(hasConstraint, thenConjunction.variables());

        Optional<Unifier> unifier = conjConcludable.unify(hasConcludable).findFirst();
        assertTrue(unifier.isPresent());
        Map<String, Set<String>> result = getStringMapping(unifier.get().mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$x", set("$p"));
            put("$y", set("$name"));
        }};
        assertTrue(result.entrySet().containsAll(expected.entrySet()));
        assertEquals(expected, result);
    }

    @Test
    public void unify_has_syntax_sugar() {
        String conjunction = "{ $x has name $y; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Has conjConcludable = concludables.iterator().next().asHas();

        Conjunction thenConjunction = parseConjunction(
                "{ $p isa $person; $p has $name; $name = 'bob' isa name;}");
        ThingVariable variable = parseThingVariable("$p has $name", "p");
        HasConstraint hasConstraint = variable.has().iterator().next();
        Rule.Conclusion.Has hasConcludable = new Rule.Conclusion.Has(hasConstraint, thenConjunction.variables());

        Optional<Unifier> unifier = conjConcludable.unify(hasConcludable).findFirst();
        assertTrue(unifier.isPresent());
        Map<String, Set<String>> result = getStringMapping(unifier.get().mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$x", set("$p"));
            put("$y", set("$name"));
        }};
        assertTrue(result.entrySet().containsAll(expected.entrySet()));
        assertEquals(expected, result);
    }

    @Test
    public void unify_relation_one_to_one_player() {
        String conjunction = "{ (employee: $x) isa employment; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction thenConjunction = parseConjunction(
                "{ ($employee: $a) isa $employment; }");
        ThingVariable variable =
                parseThingVariable("$temp ($employee: $a) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable);
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
                "{ ($employee: $a, $employee: $b, $employee: $c) isa $employment; }");
        ThingVariable variable = parseThingVariable(
                "$temp ($employee: $a, $employee: $b, $employee: $c) isa $employment",
                "temp"
        );
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable);
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

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable);
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

        Conjunction thenConjunction = parseConjunction(
                "{ $p isa $person; $p has $name; $name = 'bob' isa name;}");
        ThingVariable variable = parseThingVariable("$p has $name", "p");
        HasConstraint hasConstraint = variable.has().iterator().next();
        Rule.Conclusion.Has hasConcludable = new Rule.Conclusion.Has(hasConstraint, thenConjunction.variables());

        Optional<Unifier> unifier = conjConcludable.unify(hasConcludable).findFirst();
        assertTrue(unifier.isPresent());
        Map<String, Set<String>> result = getStringMapping(unifier.get().mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$x", set("$p", "$name"));
        }};
        assertTrue(result.entrySet().containsAll(expected.entrySet()));
        assertEquals(expected, result);
    }

    @Test
    public void has_duplicate_vars_then() {
        String conjunction = "{ $x has name $y; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Has conjConcludable = concludables.iterator().next().asHas();

        Conjunction thenConjunction = parseConjunction("{ $a has $a;}");
        ThingVariable variable = parseThingVariable("$a has $a", "a");
        HasConstraint hasConstraint = variable.has().iterator().next();
        Rule.Conclusion.Has hasConcludable = new Rule.Conclusion.Has(hasConstraint, thenConjunction.variables());

        Optional<Unifier> unifier = conjConcludable.unify(hasConcludable).findFirst();
        assertTrue(unifier.isPresent());
        Map<String, Set<String>> result = getStringMapping(unifier.get().mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$x", set("$a"));
            put("$y", set("$a"));
        }};
        assertTrue(result.entrySet().containsAll(expected.entrySet()));
        assertEquals(expected, result);
    }

    @Test
    public void has_duplicate_vars_both() {
        String conjunction = "{ $x has name $x; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Has conjConcludable = concludables.iterator().next().asHas();

        Conjunction thenConjunction = parseConjunction("{ $a has $a;}");
        ThingVariable variable = parseThingVariable("$a has $a", "a");
        HasConstraint hasConstraint = variable.has().iterator().next();
        Rule.Conclusion.Has hasConcludable = new Rule.Conclusion.Has(hasConstraint, thenConjunction.variables());

        Optional<Unifier> unifier = conjConcludable.unify(hasConcludable).findFirst();
        assertTrue(unifier.isPresent());
        Map<String, Set<String>> result = getStringMapping(unifier.get().mapping());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$x", set("$a"));
        }};
        assertTrue(result.entrySet().containsAll(expected.entrySet()));
        assertEquals(expected, result);
    }

    @Test
    public void relation_named_role() {
        String conjunction = "{ ($role: $x) isa employment; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction thenConjunction = parseConjunction(
                "{ ($employee: $a) isa $employment; }");
        ThingVariable variable =
                parseThingVariable("$temp ($employee: $a) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable);
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
    public void relation_named_role_duplication() {
        String conjunction = "{ ($role: $x) isa employment; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction thenConjunction = parseConjunction(
                "{ ($employee: $a, $employee: $b) isa $employment; }");
        ThingVariable variable =
                parseThingVariable("$temp ($employee: $a, $employee: $b) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable);
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

        Conjunction thenConjunction = parseConjunction(
                "{ ($employee: $a, $boss: $b) isa $employment; }");
        ThingVariable variable =
                parseThingVariable("$temp ($employee: $a, $employee: $b) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable);
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

        Conjunction thenConjunction = parseConjunction(
                "{$temp ($employee: $a, $boss: $a, $employee: $b) isa $employment; }");
        ThingVariable variable =
                parseThingVariable("$temp ($employee: $a, $boss: $a, $employee: $b) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable);
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

        Conjunction thenConjunction = parseConjunction(
                "{$temp ($employee: $a, $boss: $a, $employee: $b) isa $employment; }");
        ThingVariable variable =
                parseThingVariable("$temp ($employee: $a, $boss: $a, $employee: $b) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable);
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
                "{$temp ($employee: $a, $boss: $a, $employee: $b) isa $employment; }");
        ThingVariable variable =
                parseThingVariable("$temp ($employee: $a, $boss: $a, $employee: $b) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable);
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

        Conjunction thenConjunction = parseConjunction(
                "{$temp ($employee: $a, $employee: $a) isa $employment; }");
        ThingVariable variable =
                parseThingVariable("$temp ($employee: $a, $employee: $a) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable);
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

        Conjunction thenConjunction = parseConjunction(
                "{$temp ($employee: $a, $employee: $b) isa $employment; }");
        ThingVariable variable =
                parseThingVariable("$temp ($employee: $a, $employee: $b) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable);
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

        Conjunction thenConjunction = parseConjunction(
                "{$temp ($employee: $a, $employee: $a) isa $employment; }");
        ThingVariable variable =
                parseThingVariable("$temp ($employee: $a, $employee: $a) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable);
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

        Conjunction thenConjunction = parseConjunction(
                "{$temp ($employee: $a, $employee: $a) isa $employment; }");
        ThingVariable variable =
                parseThingVariable("$temp ($employee: $a, $employee: $a) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable);
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

        Conjunction thenConjunction = parseConjunction(
                "{$temp ($employee: $a) isa $employment; }");
        ThingVariable variable =
                parseThingVariable("$temp ($employee: $a) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable);
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

        Conjunction thenConjunction = parseConjunction(
                "{$temp ($employee: $a) isa $employment; }");
        ThingVariable variable =
                parseThingVariable("$temp ($employee: $a) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable);
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
        String conjunction = "{ (employee: $x, company: $y, contract: $z) isa employment; }";
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction(conjunction));
        Concludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction thenConjunction = parseConjunction(
                "{$temp ($employee: $a, $company: $b) isa $employment; }");
        ThingVariable variable =
                parseThingVariable("$temp ($employee: $a, $company: $b) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Rule.Conclusion.Relation relationConcludable = new Rule.Conclusion.Relation(relationConstraint,
                                                                                    thenConjunction.variables());

        Stream<Unifier> unifier = conjConcludable.unify(relationConcludable);
        Set<Map<String, Set<String>>> result = unifier.map(u -> getStringMapping(u.mapping())).collect(Collectors.toSet());

        Set<Map<String, Set<String>>> expected = Collections.emptySet();
        assertEquals(expected, result);
    }

}
