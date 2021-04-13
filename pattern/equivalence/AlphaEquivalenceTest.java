/*
 * Copyright (C) 2021 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.pattern.equivalence;

import grakn.common.collection.Collections;
import grakn.common.collection.Pair;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.Variable;
import graql.lang.Graql;
import graql.lang.pattern.variable.Reference;
import org.junit.Ignore;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.map;
import static grakn.core.pattern.variable.VariableRegistry.createFromVariables;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class AlphaEquivalenceTest {

    private Variable parseVariable(String variableName, String graqlVariable) {
        return createFromVariables(list(Graql.parseVariable(graqlVariable)), null).get(Reference.name(variableName));
    }

    private Variable parseVariables(String variableName, String... graqlVariables) {
        return createFromVariables(
                Arrays.stream(graqlVariables).map(Graql::parseVariable).collect(Collectors.toList()), null)
                .get(Reference.name(variableName));
    }

    private Variable parseAnonymousRelationVariable(String... graqlVariables) {
        Set<Variable> anonVariables = parseAnonymousThingVariables(graqlVariables);
        return anonVariables.stream()
                .filter(Variable::isThing)
                .filter(variable -> variable.asThing().relation().isPresent())
                .findFirst()
                .get().asThing().relation().get().owner();
    }

    private Set<Variable> parseAnonymousThingVariables(String... graqlVariables) {
        return createFromVariables(
                Arrays.stream(graqlVariables)
                        .map(Graql::parseVariable)
                        .collect(Collectors.toList()), null)
                .variables().stream()
                .filter(variable -> !variable.id().isName())
                .collect(Collectors.toSet());
    }

    private Map<String, String> alphaMapToStringMap(AlphaEquivalence.Valid alphaEq) {
        return alphaEq.variableMapping().entrySet().stream()
                .map(e -> new AbstractMap.SimpleEntry<>(
                        e.getKey().id().reference().toString(),
                        e.getValue().id().reference().toString()
                )).collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));
    }

    @Test
    public void test_isa_equivalent() {
        ThingVariable a = parseVariable("a", "$a isa age").asThing();
        ThingVariable b = parseVariable("b", "$b isa age").asThing();
        AlphaEquivalence alphaEq = a.alphaEquals(b);
        assertEquals(map(new Pair<>("$a", "$b"), new Pair<>("$_age", "$_age")), alphaMapToStringMap(alphaEq.asValid()));
        testAlphaEquivalenceSymmetricReflexive(a, b, true);
    }

    @Test
    public void test_isa_different_inequivalent_variants() {
        List<ThingVariable> variables = Stream.of(
                parseVariables("x1", "$x1 isa organisation"),
                parseVariables("x2", "$x2 isa company"),
                parseVariables("y", "$y isa $type", "$type type organisation"),
                parseVariables("z", "$z isa $y")
        )
                .map(Variable::asThing)
                .collect(Collectors.toList());
        variables.forEach(var -> testAlphaEquivalenceSymmetricReflexive(var.asThing(), variables, new HashSet<>()));
    }

    @Test
    public void test_value_and_has_label_equivalent() {
        ThingVariable p = parseVariables("p", "$p has age 30", "$p isa person").asThing();
        ThingVariable q = parseVariables("q", "$q has age 30", "$q isa person").asThing();
        AlphaEquivalence alphaEq = p.alphaEquals(q);
        Map<String, String> varNameMap = alphaMapToStringMap(alphaEq.asValid());
        assertEquals(varNameMap.get("$p"), "$q");
        assertEquals(varNameMap.get("$_age"), "$_age");
        assertEquals(varNameMap.get("$_person"), "$_person");
        testAlphaEquivalenceSymmetricReflexive(p, q, true);
    }

    @Test
    public void test_multiple_value_and_has_label_equivalent() {
        ThingVariable p = parseVariables("p", "$p has $a", "$a 30 isa age", "$p has $n", "$n \"Alice\" isa name", "$p isa person").asThing();
        ThingVariable q = parseVariables("q", "$q has $b", "$b 30 isa age", "$q has $m", "$m \"Alice\" isa name", "$q isa person").asThing();
        testAlphaEquivalenceSymmetricReflexive(p, q, true);
        AlphaEquivalence alphaEq = p.alphaEquals(q);
        Map<String, String> varNameMap = alphaMapToStringMap(alphaEq.asValid());
        assertEquals(varNameMap.get("$p"), "$q");
        assertEquals(varNameMap.get("$a"), "$b");
        assertEquals(varNameMap.get("$_age"), "$_age");
        assertEquals(varNameMap.get("$n"), "$m");
        assertEquals(varNameMap.get("$_name"), "$_name");
        assertEquals(varNameMap.get("$_person"), "$_person");
    }

    @Test
    public void test_value_not_equivalent() {
        ThingVariable p = parseVariables("p", "$p has age 30", "$p isa person").asThing();
        ThingVariable q = parseVariables("q", "$q has age 20", "$q isa person").asThing();
        testAlphaEquivalenceSymmetricReflexive(p, q, false);
    }

    @Test
    public void test_has_label_not_equivalent() {
        ThingVariable p = parseVariables("p", "$p has age 30", "$p isa person").asThing();
        ThingVariable q = parseVariables("q", "$q has years 30", "$q isa person").asThing();
        testAlphaEquivalenceSymmetricReflexive(p, q, false);
    }

    @Test
    public void test_relation_equivalent() {
        ThingVariable r = parseVariables("r", "$r(parent: $p, child: $c) isa parentship").asThing();
        ThingVariable q = parseVariables("q", "$q(parent: $s, child: $t) isa parentship").asThing();
        AlphaEquivalence alphaEq = r.alphaEquals(q);
        Map<String, String> varNameMap = alphaMapToStringMap(alphaEq.asValid());
        assertEquals(varNameMap.get("$r"), "$q");
        assertEquals(varNameMap.get("$p"), "$s");
        assertEquals(varNameMap.get("$c"), "$t");
        assertEquals(varNameMap.get("$_parentship:parent"), "$_parentship:parent");
        assertEquals(varNameMap.get("$_parentship:child"), "$_parentship:child");
        assertEquals(varNameMap.get("$_parentship"), "$_parentship");
        testAlphaEquivalenceSymmetricReflexive(r, q, true);
    }

    @Test
    public void test_relation_equivalent_with_overlapping_variables() {
        ThingVariable r = parseVariables("r", "$r(parent: $p, child: $c) isa parentship").asThing();
        ThingVariable q = parseVariables("r", "$r(parent: $s, child: $p) isa parentship").asThing();
        AlphaEquivalence alphaEq = r.alphaEquals(q);
        Map<String, String> varNameMap = alphaMapToStringMap(alphaEq.asValid());
        assertEquals(varNameMap.get("$r"), "$r");
        assertEquals(varNameMap.get("$p"), "$s");
        assertEquals(varNameMap.get("$c"), "$p");
        assertEquals(varNameMap.get("$_parentship:parent"), "$_parentship:parent");
        assertEquals(varNameMap.get("$_parentship:child"), "$_parentship:child");
        assertEquals(varNameMap.get("$_parentship"), "$_parentship");
        testAlphaEquivalenceSymmetricReflexive(r, q, true);
    }

    @Test
    public void test_relation_equivalent_with_clashing_variables() {
        ThingVariable r = parseVariables("r", "$r(parent: $x, child: $y) isa parentship").asThing();
        ThingVariable q = parseVariables("x", "$x(parent: $y, child: $r) isa parentship").asThing();
        AlphaEquivalence alphaEq = r.alphaEquals(q);
        Map<String, String> varNameMap = alphaMapToStringMap(alphaEq.asValid());
        assertEquals(varNameMap.get("$r"), "$x");
        assertEquals(varNameMap.get("$x"), "$y");
        assertEquals(varNameMap.get("$y"), "$r");
        assertEquals(varNameMap.get("$_parentship:parent"), "$_parentship:parent");
        assertEquals(varNameMap.get("$_parentship:child"), "$_parentship:child");
        assertEquals(varNameMap.get("$_parentship"), "$_parentship");
        testAlphaEquivalenceSymmetricReflexive(r, q, true);
    }

    @Test
    public void test_relation_with_duplicate_roles_equivalent() {
        ThingVariable r = parseVariables("r", "$r(sibling: $p, sibling: $c) isa siblingship", "$p isa person").asThing();
        ThingVariable q = parseVariables("q", "$q(sibling: $s, sibling: $t) isa siblingship", "$s isa person").asThing();
        testAlphaEquivalenceSymmetricReflexive(r, q, true);
        AlphaEquivalence alphaEq = r.alphaEquals(q);
        Map<String, String> varNameMap = alphaMapToStringMap(alphaEq.asValid());
        assertEquals(varNameMap.get("$r"), "$q");
        assertEquals(varNameMap.get("$p"), "$s");
        assertEquals(varNameMap.get("$c"), "$t");
        assertEquals(varNameMap.get("$_siblingship:sibling"), "$_siblingship:sibling");
        assertEquals(varNameMap.get("$_siblingship:sibling"), "$_siblingship:sibling");
        assertEquals(varNameMap.get("$_siblingship"), "$_siblingship");
    }

    @Test
    public void test_relation_with_duplicate_roleplayers_equivalent() {
        ThingVariable r = parseVariables("r", "$r(friend: $p, friend: $p) isa friendship").asThing();
        ThingVariable q = parseVariables("q", "$q(friend: $s, friend: $s) isa friendship").asThing();
        testAlphaEquivalenceSymmetricReflexive(r, q, true);
        AlphaEquivalence alphaEq = r.alphaEquals(q);
        Map<String, String> varNameMap = alphaMapToStringMap(alphaEq.asValid());
        assertEquals(varNameMap.get("$r"), "$q");
        assertEquals(varNameMap.get("$p"), "$s");
        assertEquals(varNameMap.get("$_friendship:friend"), "$_friendship:friend");
        assertEquals(varNameMap.get("$_friendship:friend"), "$_friendship:friend");
        assertEquals(varNameMap.get("$_friendship"), "$_friendship");
    }

    @Test
    public void test_relation_with_three_roles_equivalent() {
        ThingVariable r = parseVariables("r", "$r(buyer: $p, seller: $c, produce: $u) isa transaction").asThing();
        ThingVariable q = parseVariables("q", "$q(buyer: $s, seller: $t, produce: $v) isa transaction").asThing();

        AlphaEquivalence alphaEq = r.alphaEquals(q);
        Map<String, String> varNameMap = alphaMapToStringMap(alphaEq.asValid());
        assertEquals(varNameMap.get("$r"), "$q");
        assertEquals(varNameMap.get("$p"), "$s");
        assertEquals(varNameMap.get("$c"), "$t");
        assertEquals(varNameMap.get("$u"), "$v");
        assertEquals(varNameMap.get("$_transaction:buyer"), "$_transaction:buyer");
        assertEquals(varNameMap.get("$_transaction:seller"), "$_transaction:seller");
        assertEquals(varNameMap.get("$_transaction:produce"), "$_transaction:produce");
        assertEquals(varNameMap.get("$_transaction"), "$_transaction");
        testAlphaEquivalenceSymmetricReflexive(r, q, true);
    }

    @Ignore("This is outside of the scope of current alpha-equiv implementation")
    @Test
    public void test_relation_as_player_with_different_binding_configurations() {
        List<ThingVariable> variables = Stream.of(
                parseVariables("r1", "$r1 ($x)", "($r1, $y)"),
                parseVariables("r2", "$r2 ($x)", "($r2, $y)", "($r2, $z)"),
                parseVariables("r3", "$r3 ($x)", "($r3, $join)", "($r3, $join)"),
                parseVariables("r4", "$r4 ($x)", "(from: $r4, to: $x)", "(from: $r4, to: $y)"),
                parseVariables("r5", "$r5 ($x)", "(from: $r5, to: $join)", "(from: $r5, to: $join)"),
                parseVariables("r6", "$r6 ($x)", "(from: $r6, to: $join)", "(from: $join, to: $r6)")
        )
                .map(Variable::asThing)
                .collect(Collectors.toList());
        variables.forEach(var -> testAlphaEquivalenceSymmetricReflexive(var.asThing(), variables, new HashSet<>()));
    }

    @Test
    public void test_relation_with_different_roleplayer_variable_bindings() {
        List<ThingVariable> variables = Stream.of(
                parseVariables("r1", "$r1 (employer: $x, employee: $y)"),
                parseVariables("r2", "$r2 (employer: $x, employee: $x)"),
                parseVariables("r3", "$r3 (employer: $x, employee: $y, employee: $z)"),
                parseVariables("r4", "$r4 (employer: $x, employee: $x, employee: $y)")
        )
                .map(Variable::asThing)
                .collect(Collectors.toList());
        variables.forEach(var -> testAlphaEquivalenceSymmetricReflexive(var.asThing(), variables, new HashSet<>()));
    }

    @Test
    public void test_relation_with_typed_roleplayers() {
        List<ThingVariable> variables = Stream.of(
                parseAnonymousRelationVariable("($x, $y)", "$x isa organisation"),
                parseAnonymousRelationVariable("($x, $y)", "$y isa organisation"),
                parseVariables("r0", "$r0 ($x, $y)", "$x isa organisation"),
                parseVariables("r1", "$r1 ($x, $y)", "$y isa organisation"),

                parseAnonymousRelationVariable("($x, $y)", "$x isa company"),
                parseAnonymousRelationVariable("($x, $y)", "$y isa organisation", "$x isa organisation"),
                parseAnonymousRelationVariable("(employer: $x, employee: $y)"),
                parseAnonymousRelationVariable("(employer: $x, employee: $y)", "$x isa organisation"),
                parseAnonymousRelationVariable("(employer: $x, employee: $y)", "$y isa organisation"),
                parseAnonymousRelationVariable("(employer: $x, employee: $y)", "$x isa organisation", "$y isa company"),
                parseAnonymousRelationVariable("(employer: $x, employee: $y)", "$x isa organisation", "$y isa organisation"),

                parseVariables("r2", "$r2 ($x, $y)", "$x isa company"),
                parseVariables("r3", "$r3 ($x, $y)", "$y isa organisation", "$x isa organisation"),
                parseVariables("r4", "$r4 (employer: $x, employee: $y)"),
                parseVariables("r5", "$r5 (employer: $x, employee: $y)", "$x isa organisation"),
                parseVariables("r6", "$r6 (employer: $x, employee: $y)", "$y isa organisation"),
                parseVariables("r7", "$r7 (employer: $x, employee: $y)", "$x isa organisation", "$y isa company"),
                parseVariables("r8", "$r8 (employer: $x, employee: $y)", "$x isa organisation", "$y isa organisation"))
                .map(Variable::asThing)
                .collect(Collectors.toList());
        testAlphaEquivalenceSymmetricReflexive(variables.get(0), variables, Collections.set(1));
        testAlphaEquivalenceSymmetricReflexive(variables.get(1), variables, Collections.set(0));
        testAlphaEquivalenceSymmetricReflexive(variables.get(2), variables, Collections.set(3));
        testAlphaEquivalenceSymmetricReflexive(variables.get(3), variables, Collections.set(2));
        for (int vari = 4; vari < variables.size(); vari++)
            testAlphaEquivalenceSymmetricReflexive(variables.get(vari), variables, new HashSet<>());
    }

    @Test
    public void test_relation_annotated_with_attribute() {
        List<ThingVariable> variables = Stream.of(
                parseVariables("r1", "$r1 ($x, $y)"),
                parseVariables("r2", "$r2 ($x, $y)", "$r2 has $a"),
                parseVariables("r3", "$r3 ($x, $y)", "$r3 has $a", "$a 'annotation' isa annotation"),
                parseVariables("r4", "$r4 ($x, $y)", "$r4 has $a", "$a 'another annotation' isa annotation")
        )
                .map(Variable::asThing)
                .collect(Collectors.toList());
        variables.forEach(var -> testAlphaEquivalenceSymmetricReflexive(var.asThing(), variables, new HashSet<>()));
    }

    @Test
    public void test_relation_attributes_as_roleplayers() {
        List<ThingVariable> variables = Stream.of(
                parseVariables("r1", "$r1 ($x, $y)"),
                parseVariables("r2", "$r2 ($x, $y)", "$x isa name"),
                parseVariables("r3", "$r3 ($x, $y)", "$x 'Bob' isa name"),
                parseVariables("r4", "$r4 ($x, $y)", "$y 'Bob' isa name"),
                parseVariables("r5", "$r5 ($x, $y)", "$y 'Alice' isa name"),
                parseVariables("r6", "$r6 ($x, $y)", "$x 'Bob' isa name", "$y 'Alice' isa name"),
                parseVariables("r7", "$r7 ($x, $y)", "$y 'Bob' isa name", "$x 'Alice' isa name"))
                .map(Variable::asThing)
                .collect(Collectors.toList());
        testAlphaEquivalenceSymmetricReflexive(variables.get(0), variables, new HashSet<>());
        testAlphaEquivalenceSymmetricReflexive(variables.get(1), variables, new HashSet<>());
        testAlphaEquivalenceSymmetricReflexive(variables.get(2), variables, Collections.set(3));
        testAlphaEquivalenceSymmetricReflexive(variables.get(3), variables, Collections.set(2));
        testAlphaEquivalenceSymmetricReflexive(variables.get(4), variables, new HashSet<>());
        testAlphaEquivalenceSymmetricReflexive(variables.get(5), variables, Collections.set(6));
        testAlphaEquivalenceSymmetricReflexive(variables.get(6), variables, Collections.set(5));
    }

    @Test
    public void test_relation_different_inequivalent_variants() {
        List<ThingVariable> variables = Stream.of(
                parseAnonymousRelationVariable("($y)"),
                parseAnonymousRelationVariable("($y) isa $type"),
                parseAnonymousRelationVariable("($y) isa $type", "$type type parentship"),
                parseAnonymousRelationVariable("($y) isa parentship"),
                parseAnonymousRelationVariable("(parent: $y) isa parentship"),
                parseAnonymousRelationVariable("(parent: $y) isa $type", "$type type parentship"),

                parseAnonymousRelationVariable("($x, $y)"),
                parseAnonymousRelationVariable("($x, $y) isa $type"),
                parseAnonymousRelationVariable("($x, $y) isa parentship"),
                parseAnonymousRelationVariable("($x, $y) isa $type", "$type type parentship"),

                parseAnonymousRelationVariable("($y, child: $z)"),
                parseAnonymousRelationVariable("($y, child: $z) isa $type"),
                parseAnonymousRelationVariable("($y, child: $z) isa parentship"),
                parseAnonymousRelationVariable("($y, child: $z) isa $type", "$type type parentship"),

                parseAnonymousRelationVariable("(parent: $x, child: $y)"),
                parseAnonymousRelationVariable("(parent: $x, child: $y) isa $type"),
                parseAnonymousRelationVariable("(parent: $x, child: $y) isa parentship"),
                parseAnonymousRelationVariable("(parent: $x, child: $y) isa $type", "$type type parentship"),
                parseAnonymousRelationVariable("(parent: $x, child: $y, $z) isa parentship"),

                parseVariables("r0", "$r0 ($y)"),
                parseVariables("r1", "$r1 ($y) isa $type"),
                parseVariables("r2", "$r2 (parent: $y) isa parentship"),
                parseVariables("r3", "$r3 (parent: $y) isa $type", "$type type parentship"),
                parseVariables("r4", "$r4 (parent: $y, child: $z) isa parentship"),
                parseVariables("r5", "$r5 (parent: $y, child: $z) isa $pship", "$pship type parentship"),
                parseVariables("r6", "$r6 ($y, $z) isa parentship"),
                parseVariables("r7", "$r7 (parent: $x, child: $y, $z) isa parentship"),
                parseVariables("r8", "$r8 ($y, $z, $u) isa parentship")
        )
                .map(Variable::asThing)
                .collect(Collectors.toList());
        variables.forEach(var -> testAlphaEquivalenceSymmetricReflexive(var.asThing(), variables, new HashSet<>()));
    }

    private void testAlphaEquivalenceSymmetricReflexive(ThingVariable varA, ThingVariable varB, boolean isValid) {
        assertTrue(varA.alphaEquals(varA).isValid());
        assertTrue(varB.alphaEquals(varB).isValid());
        assertEquals("Variable:\n" + varA + "\n=?\n" + varB, isValid, varA.alphaEquals(varB).isValid());
        assertEquals("Variable:\n" + varB + "\n=?\n" + varA, isValid, varB.alphaEquals(varA).isValid());
    }

    private void testAlphaEquivalenceSymmetricReflexive(ThingVariable sourceVar, List<ThingVariable> toCheck, Set<Integer> validVars) {
        //use var index as .equals on Variable doesn't seem reliable
        for (int varIndex = 0; varIndex < toCheck.size(); varIndex++) {
            ThingVariable var = toCheck.get(varIndex);
            //compare only by reference
            if (var != sourceVar) {
                testAlphaEquivalenceSymmetricReflexive(sourceVar, var, validVars.contains(varIndex));
            }
        }
    }
}
