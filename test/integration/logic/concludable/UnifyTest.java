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

public class UnifyTest {

    private Map<String, Set<String>> getStringMapping(Map<Reference, Set<Reference>> map) {
        return map.entrySet().stream().collect(Collectors.toMap(
                v -> v.getKey().syntax(),
                e -> e.getValue().stream().map(Reference::syntax).collect(Collectors.toSet())
                )
        );
    }

    private Conjunction parseConjunction(String query) {
        return Disjunction.create(Graql.parsePattern(query).asConjunction().normalise()).conjunctions().iterator().next();
    }

    private ThingVariable parseThingVariable(String graqlVariable, String variableName) {
        return createFromThings(list(Graql.parseVariable(graqlVariable).asThing())).get(Reference.named(variableName)).asThing();
    }

    //TESTS START
    //TODO: create more tests when type inference is working.
    @Test
    public void unify_isa_variable() {
        String conjunction = "{ $x isa $y; }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.of(parseConjunction(conjunction));
        ConjunctionConcludable.Isa conjConcludable = concludables.iterator().next().asIsa();

        Conjunction headConjunction = parseConjunction("{ $a 7; $a isa $age; }");
        ThingVariable variable = parseThingVariable("$a isa $age", "a");
        assertTrue(variable.isa().isPresent());
        IsaConstraint isaConstraint = variable.isa().get();
        HeadConcludable.Isa isaConcludable = new HeadConcludable.Isa(isaConstraint, headConjunction.variables());

        Optional<Map<Reference, Set<Reference>>> unifier = conjConcludable.unify(isaConcludable).findFirst();
        assertTrue(unifier.isPresent());
        Map<String, Set<String>> result = getStringMapping(unifier.get());
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
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.of(parseConjunction(conjunction));
        ConjunctionConcludable.Isa conjConcludable = concludables.iterator().next().asIsa();

        Conjunction headConjunction = parseConjunction("{ $a 7; $a isa $person; }");
        ThingVariable variable = parseThingVariable("$a isa $person", "a");
        assertTrue(variable.isa().isPresent());
        IsaConstraint isaConstraint = variable.isa().get();
        HeadConcludable.Isa isaConcludable = new HeadConcludable.Isa(isaConstraint, headConjunction.variables());

        Optional<Map<Reference, Set<Reference>>> unifier = conjConcludable.unify(isaConcludable).findFirst();
        assertTrue(unifier.isPresent());
        Map<String, Set<String>> result = getStringMapping(unifier.get());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$x", set("$a"));
        }};
        assertTrue(result.entrySet().containsAll(expected.entrySet()));
        assertEquals(expected, result);
    }

    @Test
    public void unify_value_concrete() {
        String conjunction = "{ $x = 7; }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.of(parseConjunction(conjunction));
        ConjunctionConcludable.Value conjConcludable = concludables.iterator().next().asValue();

        Conjunction headConjunction = parseConjunction("{ $a = $b; $a isa $person; $num = 7; }");
        ThingVariable variable = parseThingVariable("$a = 7", "a");
        ValueConstraint<?> valueConstraint = variable.value().iterator().next();
        HeadConcludable.Value valueConcludable = new HeadConcludable.Value(valueConstraint, headConjunction.variables());

        Optional<Map<Reference, Set<Reference>>> unifier = conjConcludable.unify(valueConcludable).findFirst();
        assertTrue(unifier.isPresent());
        Map<String, Set<String>> result = getStringMapping(unifier.get());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$x", set("$a"));
        }};
        assertTrue(result.entrySet().containsAll(expected.entrySet()));
        assertEquals(expected, result);
    }

    @Test
    public void unify_value_predicate() {
        String conjunction = "{ $x > 7; }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.of(parseConjunction(conjunction));
        ConjunctionConcludable.Value conjConcludable = concludables.iterator().next().asValue();

        Conjunction headConjunction = parseConjunction("{ $a > $num; $a isa $person; $num = 7; }");
        ThingVariable variable = parseThingVariable("$a > $num", "a");
        ValueConstraint<?> valueConstraint = variable.value().iterator().next();
        HeadConcludable.Value valueConcludable = new HeadConcludable.Value(valueConstraint, headConjunction.variables());

        Optional<Map<Reference, Set<Reference>>> unifier = conjConcludable.unify(valueConcludable).findFirst();
        assertTrue(unifier.isPresent());
        Map<String, Set<String>> result = getStringMapping(unifier.get());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$x", set("$a"));
        }};
        assertTrue(result.entrySet().containsAll(expected.entrySet()));
        assertEquals(expected, result);
    }

    @Test
    public void unify_value_variable() {
        String conjunction = "{ $x > $y; }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.of(parseConjunction(conjunction));
        ConjunctionConcludable.Value conjConcludable = concludables.iterator().next().asValue();

        Conjunction headConjunction = parseConjunction("{ $a > $num; $a isa $person; $num = 7; }");
        ThingVariable variable = parseThingVariable("$a > $num", "a");
        ValueConstraint<?> valueConstraint = variable.value().iterator().next();
        HeadConcludable.Value valueConcludable = new HeadConcludable.Value(valueConstraint, headConjunction.variables());

        Optional<Map<Reference, Set<Reference>>> unifier = conjConcludable.unify(valueConcludable).findFirst();
        assertTrue(unifier.isPresent());
        Map<String, Set<String>> result = getStringMapping(unifier.get());
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
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.of(parseConjunction(conjunction));
        ConjunctionConcludable.Has conjConcludable = concludables.iterator().next().asHas();

        Conjunction headConjunction = parseConjunction(
                "{ $p isa $person; $p has $name; $name = 'bob' isa name;}");
        ThingVariable variable = parseThingVariable("$p has $name", "p");
        HasConstraint hasConstraint = variable.has().iterator().next();
        HeadConcludable.Has hasConcludable = new HeadConcludable.Has(hasConstraint, headConjunction.variables());

        Optional<Map<Reference, Set<Reference>>> unifier = conjConcludable.unify(hasConcludable).findFirst();
        assertTrue(unifier.isPresent());
        Map<String, Set<String>> result = getStringMapping(unifier.get());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$x", set("$p"));
        }};
        assertTrue(result.entrySet().containsAll(expected.entrySet()));
        assertEquals(expected, result);
    }

    @Test
    public void unify_has_variable() {
        String conjunction = "{ $x has $y; }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.of(parseConjunction(conjunction));
        ConjunctionConcludable.Has conjConcludable = concludables.iterator().next().asHas();

        Conjunction headConjunction = parseConjunction(
                "{ $p isa $person; $p has $name; $name = 'bob' isa name;}");
        ThingVariable variable = parseThingVariable("$p has $name", "p");
        HasConstraint hasConstraint = variable.has().iterator().next();
        HeadConcludable.Has hasConcludable = new HeadConcludable.Has(hasConstraint, headConjunction.variables());

        Optional<Map<Reference, Set<Reference>>> unifier = conjConcludable.unify(hasConcludable).findFirst();
        assertTrue(unifier.isPresent());
        Map<String, Set<String>> result = getStringMapping(unifier.get());
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
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.of(parseConjunction(conjunction));
        ConjunctionConcludable.Has conjConcludable = concludables.iterator().next().asHas();

        Conjunction headConjunction = parseConjunction(
                "{ $p isa $person; $p has $name; $name = 'bob' isa name;}");
        ThingVariable variable = parseThingVariable("$p has $name", "p");
        HasConstraint hasConstraint = variable.has().iterator().next();
        HeadConcludable.Has hasConcludable = new HeadConcludable.Has(hasConstraint, headConjunction.variables());

        Optional<Map<Reference, Set<Reference>>> unifier = conjConcludable.unify(hasConcludable).findFirst();
        assertTrue(unifier.isPresent());
        Map<String, Set<String>> result = getStringMapping(unifier.get());
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
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.of(parseConjunction(conjunction));
        ConjunctionConcludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction headConjunction = parseConjunction(
                "{ ($employee: $a) isa $employment; }");
        ThingVariable variable =
                parseThingVariable("$temp ($employee: $a) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        HeadConcludable.Relation relationConcludable = new HeadConcludable.Relation(relationConstraint,
                headConjunction.variables());

        Stream<Map<Reference, Set<Reference>>> unifier = conjConcludable.unify(relationConcludable);
        Set<Map<String, Set<String>>> result = unifier.map(this::getStringMapping).collect(Collectors.toSet());

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
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.of(parseConjunction(conjunction));
        ConjunctionConcludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction headConjunction = parseConjunction(
                "{ ($employee: $a, $employee: $b, $employee: $c) isa $employment; }");
        ThingVariable variable = parseThingVariable(
                "$temp ($employee: $a, $employee: $b, $employee: $c) isa $employment",
                "temp"
        );
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        HeadConcludable.Relation relationConcludable = new HeadConcludable.Relation(relationConstraint,
                headConjunction.variables());

        Stream<Map<Reference, Set<Reference>>> unifier = conjConcludable.unify(relationConcludable);
        Set<Map<String, Set<String>>> result = unifier.map(this::getStringMapping).collect(Collectors.toSet());

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
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.of(parseConjunction(conjunction));
        ConjunctionConcludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction headConjunction = parseConjunction(
                "{ ($employee: $a, $employee: $b, $employee: $c) isa $employment; }");
        ThingVariable variable = parseThingVariable(
                "$temp ($employee: $a, $employee: $b, $employee: $c) isa $employment",
                "temp"
        );
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        HeadConcludable.Relation relationConcludable = new HeadConcludable.Relation(relationConstraint,
                headConjunction.variables());

        Stream<Map<Reference, Set<Reference>>> unifier = conjConcludable.unify(relationConcludable);
        Set<Map<String, Set<String>>> result = unifier.map(this::getStringMapping).collect(Collectors.toSet());

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
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.of(parseConjunction(conjunction));
        ConjunctionConcludable.Has conjConcludable = concludables.iterator().next().asHas();

        Conjunction headConjunction = parseConjunction(
                "{ $p isa $person; $p has $name; $name = 'bob' isa name;}");
        ThingVariable variable = parseThingVariable("$p has $name", "p");
        HasConstraint hasConstraint = variable.has().iterator().next();
        HeadConcludable.Has hasConcludable = new HeadConcludable.Has(hasConstraint, headConjunction.variables());

        Optional<Map<Reference, Set<Reference>>> unifier = conjConcludable.unify(hasConcludable).findFirst();
        assertTrue(unifier.isPresent());
        Map<String, Set<String>> result = getStringMapping(unifier.get());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$x", set("$p", "$name"));
        }};
        assertTrue(result.entrySet().containsAll(expected.entrySet()));
        assertEquals(expected, result);
    }

    @Test
    public void has_duplicate_vars_head() {
        String conjunction = "{ $x has name $y; }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.of(parseConjunction(conjunction));
        ConjunctionConcludable.Has conjConcludable = concludables.iterator().next().asHas();

        Conjunction headConjunction = parseConjunction("{ $a has $a;}");
        ThingVariable variable = parseThingVariable("$a has $a", "a");
        HasConstraint hasConstraint = variable.has().iterator().next();
        HeadConcludable.Has hasConcludable = new HeadConcludable.Has(hasConstraint, headConjunction.variables());

        Optional<Map<Reference, Set<Reference>>> unifier = conjConcludable.unify(hasConcludable).findFirst();
        assertTrue(unifier.isPresent());
        Map<String, Set<String>> result = getStringMapping(unifier.get());
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
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.of(parseConjunction(conjunction));
        ConjunctionConcludable.Has conjConcludable = concludables.iterator().next().asHas();

        Conjunction headConjunction = parseConjunction("{ $a has $a;}");
        ThingVariable variable = parseThingVariable("$a has $a", "a");
        HasConstraint hasConstraint = variable.has().iterator().next();
        HeadConcludable.Has hasConcludable = new HeadConcludable.Has(hasConstraint, headConjunction.variables());

        Optional<Map<Reference, Set<Reference>>> unifier = conjConcludable.unify(hasConcludable).findFirst();
        assertTrue(unifier.isPresent());
        Map<String, Set<String>> result = getStringMapping(unifier.get());
        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$x", set("$a"));
        }};
        assertTrue(result.entrySet().containsAll(expected.entrySet()));
        assertEquals(expected, result);
    }

    @Test
    public void relation_named_role() {
        String conjunction = "{ ($role: $x) isa employment; }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.of(parseConjunction(conjunction));
        ConjunctionConcludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction headConjunction = parseConjunction(
                "{ ($employee: $a) isa $employment; }");
        ThingVariable variable =
                parseThingVariable("$temp ($employee: $a) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        HeadConcludable.Relation relationConcludable = new HeadConcludable.Relation(relationConstraint,
                headConjunction.variables());

        Stream<Map<Reference, Set<Reference>>> unifier = conjConcludable.unify(relationConcludable);
        Set<Map<String, Set<String>>> result = unifier.map(this::getStringMapping).collect(Collectors.toSet());

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
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.of(parseConjunction(conjunction));
        ConjunctionConcludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction headConjunction = parseConjunction(
                "{ ($employee: $a, $employee: $b) isa $employment; }");
        ThingVariable variable =
                parseThingVariable("$temp ($employee: $a, $employee: $b) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        HeadConcludable.Relation relationConcludable = new HeadConcludable.Relation(relationConstraint,
                headConjunction.variables());

        Stream<Map<Reference, Set<Reference>>> unifier = conjConcludable.unify(relationConcludable);
        Set<Map<String, Set<String>>> result = unifier.map(this::getStringMapping).collect(Collectors.toSet());

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
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.of(parseConjunction(conjunction));
        ConjunctionConcludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction headConjunction = parseConjunction(
                "{ ($employee: $a, $boss: $b) isa $employment; }");
        ThingVariable variable =
                parseThingVariable("$temp ($employee: $a, $employee: $b) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        HeadConcludable.Relation relationConcludable = new HeadConcludable.Relation(relationConstraint,
                headConjunction.variables());

        Stream<Map<Reference, Set<Reference>>> unifier = conjConcludable.unify(relationConcludable);
        Set<Map<String, Set<String>>> result = unifier.map(this::getStringMapping).collect(Collectors.toSet());

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
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.of(parseConjunction(conjunction));
        ConjunctionConcludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction headConjunction = parseConjunction(
                "{$temp ($employee: $a, $boss: $a, $employee: $b) isa $employment; }");
        ThingVariable variable =
                parseThingVariable("$temp ($employee: $a, $boss: $a, $employee: $b) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        HeadConcludable.Relation relationConcludable = new HeadConcludable.Relation(relationConstraint,
                headConjunction.variables());

        Stream<Map<Reference, Set<Reference>>> unifier = conjConcludable.unify(relationConcludable);
        Set<Map<String, Set<String>>> result = unifier.map(this::getStringMapping).collect(Collectors.toSet());

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
    public void relation_repeated_players_many_to_many2() {
        String conjunction = "{ ($role1: $x, $role2: $y, $role1: $x) isa employment; }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.of(parseConjunction(conjunction));
        ConjunctionConcludable.Relation conjConcludable = concludables.iterator().next().asRelation();

        Conjunction headConjunction = parseConjunction(
                "{$temp ($employee: $a, $boss: $a, $company: $b) isa $employment; }");
        ThingVariable variable =
                parseThingVariable("$temp ($employee: $a, $boss: $a, $employee: $b) isa $employment", "temp");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        HeadConcludable.Relation relationConcludable = new HeadConcludable.Relation(relationConstraint,
                headConjunction.variables());

        Stream<Map<Reference, Set<Reference>>> unifier = conjConcludable.unify(relationConcludable);
        Set<Map<String, Set<String>>> result = unifier.map(this::getStringMapping).collect(Collectors.toSet());

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

}
