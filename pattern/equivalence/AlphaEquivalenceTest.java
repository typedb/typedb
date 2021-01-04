/*
 * Copyright (C) 2020 Grakn Labs
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

import grakn.common.collection.Pair;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.Variable;
import graql.lang.Graql;
import graql.lang.pattern.variable.Reference;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.map;
import static grakn.core.pattern.variable.VariableRegistry.createFromVariables;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class AlphaEquivalenceTest {

    private Variable parseVariable(String variableName, String graqlVariable) {
        return createFromVariables(list(Graql.parseVariable(graqlVariable)), null).get(Reference.named(variableName));
    }

    private Variable parseVariables(String variableName, String... graqlVariables) {
        return createFromVariables(
                Arrays.stream(graqlVariables).map(Graql::parseVariable).collect(Collectors.toList()), null)
                .get(Reference.named(variableName));
    }

    private Set<Variable> parseAnonymousThingVariables(String... graqlVariables) {
        return createFromVariables(
                Arrays.stream(graqlVariables)
                        .map(graqlVariable -> Graql.parseVariable(graqlVariable).asThing())
                        .collect(Collectors.toList()), null)
                .variables().stream()
                .filter(variable -> !variable.id().isNamedReference())
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
        assertTrue(alphaEq.isValid());
        assertEquals(map(new Pair<>("$a", "$b"), new Pair<>("$_age", "$_age")), alphaMapToStringMap(alphaEq.asValid()));
    }

    @Test
    public void test_isa_not_equivalent() {
        ThingVariable a = parseVariable("a", "$a isa age").asThing();
        ThingVariable b = parseVariable("b", "$b isa person").asThing();
        assertFalse(a.alphaEquals(b).isValid());
    }

    @Test
    public void test_value_and_has_label_equivalent() {
        ThingVariable p = parseVariables("p", "$p has age 30", "$p isa person").asThing();
        ThingVariable q = parseVariables("q", "$q has age 30", "$q isa person").asThing();
        AlphaEquivalence alphaEq = p.alphaEquals(q);
        assertTrue(alphaEq.isValid());
        Map<String, String> varNameMap = alphaMapToStringMap(alphaEq.asValid());
        assertEquals(varNameMap.get("$p"), "$q");
        assertEquals(varNameMap.get("$_age"), "$_age");
        assertEquals(varNameMap.get("$_person"), "$_person");
    }

    @Test
    public void test_multiple_value_and_has_label_equivalent() {
        ThingVariable p = parseVariables("p", "$p has $a", "$a 30 isa age", "$p has $n", "$n \"Alice\" isa name", "$p isa person").asThing();
        ThingVariable q = parseVariables("q", "$q has $b", "$b 30 isa age", "$q has $m", "$m \"Alice\" isa name", "$q isa person").asThing();
        AlphaEquivalence alphaEq = p.alphaEquals(q);
        assertTrue(alphaEq.isValid());
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
        assertFalse(p.alphaEquals(q).isValid());
    }

    @Test
    public void test_has_label_not_equivalent() {
        ThingVariable p = parseVariables("p", "$p has age 30", "$p isa person").asThing();
        ThingVariable q = parseVariables("q", "$q has years 30", "$q isa person").asThing();
        assertFalse(p.alphaEquals(q).isValid());
    }

    @Test
    public void test_relation_equivalent() {
        ThingVariable r = parseVariables("r", "$r(parent: $p, child: $c) isa parentship").asThing();
        ThingVariable q = parseVariables("q", "$q(parent: $s, child: $t) isa parentship").asThing();
        assertTrue(r.alphaEquals(q).isValid());
        AlphaEquivalence alphaEq = r.alphaEquals(q);
        assertTrue(alphaEq.isValid());
        Map<String, String> varNameMap = alphaMapToStringMap(alphaEq.asValid());
        assertEquals(varNameMap.get("$r"), "$q");
        assertEquals(varNameMap.get("$p"), "$s");
        assertEquals(varNameMap.get("$c"), "$t");
        assertEquals(varNameMap.get("$_parentship:parent"), "$_parentship:parent");
        assertEquals(varNameMap.get("$_parentship:child"), "$_parentship:child");
        assertEquals(varNameMap.get("$_parentship"), "$_parentship");
    }

    @Test
    public void test_relation_equivalent_with_overlapping_variables() {
        ThingVariable r = parseVariables("r", "$r(parent: $p, child: $c) isa parentship").asThing();
        ThingVariable q = parseVariables("r", "$r(parent: $s, child: $p) isa parentship").asThing();
        assertTrue(r.alphaEquals(q).isValid());
        AlphaEquivalence alphaEq = r.alphaEquals(q);
        assertTrue(alphaEq.isValid());
        Map<String, String> varNameMap = alphaMapToStringMap(alphaEq.asValid());
        assertEquals(varNameMap.get("$r"), "$r");
        assertEquals(varNameMap.get("$p"), "$s");
        assertEquals(varNameMap.get("$c"), "$p");
        assertEquals(varNameMap.get("$_parentship:parent"), "$_parentship:parent");
        assertEquals(varNameMap.get("$_parentship:child"), "$_parentship:child");
        assertEquals(varNameMap.get("$_parentship"), "$_parentship");
    }

    @Test
    public void test_relation_with_duplicate_roles_equivalent() {
        ThingVariable r = parseVariables("r", "$r(sibling: $p, sibling: $c) isa siblingship", "$p isa person").asThing();
        ThingVariable q = parseVariables("q", "$q(sibling: $s, sibling: $t) isa siblingship", "$s isa person").asThing();
        assertTrue(r.alphaEquals(q).isValid());
        AlphaEquivalence alphaEq = r.alphaEquals(q);
        assertTrue(alphaEq.isValid());
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
        assertTrue(r.alphaEquals(q).isValid());
        AlphaEquivalence alphaEq = r.alphaEquals(q);
        assertTrue(alphaEq.isValid());
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
        assertTrue(r.alphaEquals(q).isValid());
        AlphaEquivalence alphaEq = r.alphaEquals(q);
        assertTrue(alphaEq.isValid());
        Map<String, String> varNameMap = alphaMapToStringMap(alphaEq.asValid());
        assertEquals(varNameMap.get("$r"), "$q");
        assertEquals(varNameMap.get("$p"), "$s");
        assertEquals(varNameMap.get("$c"), "$t");
        assertEquals(varNameMap.get("$u"), "$v");
        assertEquals(varNameMap.get("$_transaction:buyer"), "$_transaction:buyer");
        assertEquals(varNameMap.get("$_transaction:seller"), "$_transaction:seller");
        assertEquals(varNameMap.get("$_transaction:produce"), "$_transaction:produce");
        assertEquals(varNameMap.get("$_transaction"), "$_transaction");
    }

    @Test
    public void test_relation_role_type_not_equivalent() {
        ThingVariable r = parseVariables("r", "$r(parent: $p, child: $c) isa parentship").asThing();
        ThingVariable q = parseVariables("q", "$q(grandparent: $s, child: $t) isa parentship").asThing();
        assertFalse(r.alphaEquals(q).isValid());
    }

    @Test
    public void test_relation_with_different_player_isa_not_equivalent() {
        ThingVariable r = parseVariables("r", "$r(parent: $p, child: $c) isa parentship", "$p isa person").asThing();
        ThingVariable q = parseVariables("q", "$q(parent: $s, child: $t) isa parentship").asThing();
        assertFalse(r.alphaEquals(q).isValid());
    }

    @Test
    public void test_relation_with_missing_role_type_not_equivalent() {
        ThingVariable r = parseVariables("r", "$r(parent: $p, child: $c) isa parentship").asThing();
        ThingVariable q = parseVariables("q", "$q(parent: $s, $c) isa parentship").asThing();
        assertFalse(r.alphaEquals(q).isValid());
    }

    @Test
    public void test_relation_with_missing_player_not_equivalent() {
        ThingVariable r = parseVariables("r", "$r(parent: $p, child: $c) isa parentship").asThing();
        ThingVariable q = parseVariables("q", "$q(parent: $s) isa parentship").asThing();
        assertFalse(r.alphaEquals(q).isValid());
    }

    @Test
    public void test_relation_with_unnamed_thing_variable_not_equivalent() {
        ThingVariable r = parseVariables("r", "$r(parent: $p, child: $c) isa parentship").asThing();
        Set<Variable> anonVariables = parseAnonymousThingVariables("(parent: $s, child: $t) isa parentship");
        ThingVariable q = anonVariables.stream().filter(Variable::isThing).filter(variable -> variable.asThing().relation().size() > 0).findFirst().get().asThing().relation().iterator().next().owner();
        assertFalse(r.alphaEquals(q).isValid());
    }

    @Test
    public void test_relation_with_unnamed_type_variable_not_equivalent() {
        ThingVariable r = parseVariables("r", "$r(parent: $p, child: $c) isa parentship").asThing();
        ThingVariable q = parseVariables("q", "$q(parent: $s, child: $t) isa $pship", "$pship type parentship").asThing();
        assertFalse(r.alphaEquals(q).isValid());
    }
}
