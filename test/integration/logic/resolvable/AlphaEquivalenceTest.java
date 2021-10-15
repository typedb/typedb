/*
 * Copyright (C) 2021 Vaticle
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
 *
 */

package com.vaticle.typedb.core.logic.resolvable;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.equivalence.AlphaEquivalence;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;
import com.vaticle.typedb.core.rocks.RocksTypeDB;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.variable.BoundVariable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class AlphaEquivalenceTest {

    private static final String database = "AlphaEquivalenceTest";
    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve(database);
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).logsDir(logDir);
    private RocksTypeDB typeDB;

    @Before
    public void setUp() throws IOException {
        Util.resetDirectory(dataDir);
        this.typeDB = RocksTypeDB.open(options);
        this.typeDB.databases().create(database);
    }

    @After
    public void tearDown() {
        this.typeDB.close();
    }

    private void schema(String schema) {
        try (TypeDB.Session session = typeDB.session(AlphaEquivalenceTest.database, Arguments.Session.Type.SCHEMA)) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                tx.query().define(TypeQL.parseQuery(String.join("\n", schema)).asDefine());
                tx.commit();
            }
        }
    }

    private void typeInference(Conjunction conjunction) {
        try (TypeDB.Session session = typeDB.session(AlphaEquivalenceTest.database, Arguments.Session.Type.DATA)) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                tx.logic().typeInference().infer(conjunction, false);
            }
        }
    }

    private static Map<String, String> alphaMapToStringMap(AlphaEquivalence alphaEq) {
        return alphaEq.variableMapping().entrySet().stream()
                .map(e -> new AbstractMap.SimpleEntry<>(
                        e.getKey().id().reference().toString(),
                        e.getValue().id().reference().toString()
                )).collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));
    }

    private Concludable concludable(String variableString) {
        BoundVariable vars = TypeQL.parseVariable(variableString).asVariable();
        VariableRegistry registry = VariableRegistry.createFromVariables(list(vars), null);
        Conjunction conjunction = new Conjunction(registry.variables(), set());
        typeInference(conjunction);
        Set<Concludable> concludables = Concludable.create(conjunction);
        assert concludables.size() == 1;
        return Iterators.iterate(concludables).next();
    }

    private Set<Concludable> concludable(String... variableStrings) {
        List<BoundVariable> variables = new ArrayList<>();
        for (String variableString : variableStrings) variables.add(TypeQL.parseVariable(variableString).asVariable());
        VariableRegistry registry = VariableRegistry.createFromVariables(variables, null);
        Conjunction conjunction = new Conjunction(registry.variables(), set());
        typeInference(conjunction);
        return Concludable.create(conjunction);
    }

    private static Concludable.Relation relation(Set<Concludable> concludables) {
        return Iterators.iterate(concludables).filter(Concludable::isRelation).first().get().asRelation();
    }

    private static Concludable.Has has(Set<Concludable> concludables) {
        return Iterators.iterate(concludables).filter(Concludable::isHas).first().get().asHas();
    }

    private static Concludable.Isa isa(Set<Concludable> concludables) {
        return Iterators.iterate(concludables).filter(Concludable::isIsa).first().get().asIsa();
    }

    private static Concludable.Attribute attribute(Set<Concludable> concludables) {
        return Iterators.iterate(concludables).filter(Concludable::isAttribute).first().get().asAttribute();
    }

    private static Map<String, String> singleAlphaMap(Concludable r, Concludable q) {
        FunctionalIterator<AlphaEquivalence> equivalences = r.alphaEquals(q);
        AlphaEquivalence equivalence = equivalences.next();
        assertFalse(equivalences.hasNext());
        return alphaMapToStringMap(equivalence);
    }

    @Test
    public void test_isa_equivalent() {
        schema("define age sub attribute, value long;");
        Concludable a = concludable("$a isa age");
        Concludable b = concludable("$b isa age");
        Map<String, String> varNameMap = singleAlphaMap(a, b);
        assertEquals(varNameMap.get("$a"), "$b");
        assertEquals(varNameMap.get("$_age"), "$_age");
        testAlphaEquivalenceSymmetricReflexive(a, b, true);
    }

    @Test
    public void test_isa_different_inequivalent_variants() {
        schema("define organisation sub entity; company sub entity;");
        List<Concludable> variables = Iterators.iterate(
                concludable("$x1 isa organisation"),
                concludable("$x2 isa company"),
                isa(concludable("$y isa $type", "$type type organisation")),
                concludable("$z isa $y")
        ).toList();
        variables.forEach(var -> testAlphaEquivalenceSymmetricReflexive(var, variables, new HashSet<>()));
    }

    @Test
    public void test_value_and_has_label_equivalent() {
        schema("define person sub entity, owns age; age sub attribute, value long;");
        Concludable p = has(concludable("$p has age 30", "$p isa person"));
        Concludable q = has(concludable("$q has age 30", "$q isa person"));
        Map<String, String> varNameMap = singleAlphaMap(p, q);
        assertEquals("$q", varNameMap.get("$p"));
        assertEquals("$_age", varNameMap.get("$_age"));
        testAlphaEquivalenceSymmetricReflexive(p, q, true);
    }

    @Test
    public void test_multiple_value_and_has_label_equivalent() {
        schema("define person sub entity, owns age; age sub attribute, value long;");
        Concludable p = has(concludable("$p has $a", "$a isa age", "$a > 29", "$a < 31", "$p isa person"));
        Concludable q = has(concludable("$q has $b", "$b isa age", "$b > 29", "$b < 31", "$q isa person"));
        Map<String, String> varNameMap = singleAlphaMap(p, q);
        assertEquals(varNameMap.get("$p"), "$q");
        assertEquals(varNameMap.get("$a"), "$b");
        assertEquals(varNameMap.get("$_age"), "$_age");
        testAlphaEquivalenceSymmetricReflexive(p, q, true);
    }

    @Test
    public void test_value_not_equivalent() {
        schema("define person sub entity, owns age; age sub attribute, value long;");
        Concludable p = has(concludable("$p has age 30", "$p isa person"));
        Concludable q = has(concludable("$q has age 20", "$q isa person"));
        testAlphaEquivalenceSymmetricReflexive(p, q, false);
    }

    @Test
    public void test_has_label_not_equivalent() {
        schema("define person sub entity, owns age, owns years; age sub attribute, value long; years sub attribute, value long;");
        Concludable p = has(concludable("$p has age 30", "$p isa person"));
        Concludable q = has(concludable("$q has years 30", "$q isa person"));
        testAlphaEquivalenceSymmetricReflexive(p, q, false);
    }

    @Test
    public void test_relation_equivalent() {
        schema("define parentship sub relation, relates parent, relates child; " +
                       "person sub entity, plays parentship:parent, plays parentship:child;");
        Concludable r = concludable("$r(parent: $p, child: $c) isa parentship");
        Concludable q = concludable("$q(parent: $s, child: $t) isa parentship");
        Map<String, String> varNameMap = singleAlphaMap(r, q);
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
        schema("define parentship sub relation, relates parent, relates child; " +
                       "person sub entity, plays parentship:parent, plays parentship:child;");
        Concludable r = concludable("$r(parent: $p, child: $c) isa parentship");
        Concludable q = concludable("$r(parent: $s, child: $p) isa parentship");
        Map<String, String> varNameMap = singleAlphaMap(r, q);
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
        schema("define parentship sub relation, relates parent, relates child; " +
                       "person sub entity, plays parentship:parent, plays parentship:child;");
        Concludable r = concludable("$r(parent: $x, child: $y) isa parentship");
        Concludable q = concludable("$x(parent: $y, child: $r) isa parentship");
        Map<String, String> varNameMap = singleAlphaMap(r, q);
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
        schema("define siblingship sub relation, relates sibling; person sub entity, plays siblingship:sibling;");
        Concludable r = relation(concludable("$r(sibling: $p, sibling: $c) isa siblingship", "$p isa person"));
        Concludable q = relation(concludable("$q(sibling: $s, sibling: $t) isa siblingship", "$s isa person"));
        Set<Map<String, String>> alphaMaps = r.alphaEquals(q).map(AlphaEquivalenceTest::alphaMapToStringMap).toSet();
        Map<String, String> map1 = new HashMap<String, String>() {{
            put("$r", "$q");
            put("$p", "$s");
            put("$c", "$t");
            put("$_siblingship:sibling", "$_siblingship:sibling");
            put("$_siblingship", "$_siblingship");
        }};
        Map<String, String> map2 = new HashMap<String, String>() {{
            put("$r", "$q");
            put("$p", "$t");
            put("$c", "$s");
            put("$_siblingship:sibling", "$_siblingship:sibling");
            put("$_siblingship", "$_siblingship");
        }};
        assertEquals(set(map1, map2), alphaMaps);
        testAlphaEquivalenceSymmetricReflexive(r, q, true);
    }

    @Test
    public void test_relation_with_duplicate_roleplayers_equivalent() {
        schema("define friendship sub relation, relates friend; " +
                       "person sub entity, plays friendship:friend;");
        Concludable r = concludable("$r(friend: $p, friend: $p) isa friendship");
        Concludable q = concludable("$q(friend: $s, friend: $s) isa friendship");
        Map<String, String> varNameMap = singleAlphaMap(r, q);
        assertEquals(varNameMap.get("$r"), "$q");
        assertEquals(varNameMap.get("$p"), "$s");
        assertEquals(varNameMap.get("$_friendship:friend"), "$_friendship:friend");
        assertEquals(varNameMap.get("$_friendship:friend"), "$_friendship:friend");
        assertEquals(varNameMap.get("$_friendship"), "$_friendship");
        testAlphaEquivalenceSymmetricReflexive(r, q, true);
    }

    @Test
    public void test_relation_with_three_roles_equivalent() {
        schema("define transaction sub relation, relates buyer, relates seller, relates produce; " +
                       "something sub entity, plays transaction:buyer, plays transaction:seller, plays transaction:produce;");
        Concludable r = concludable("$r(buyer: $p, seller: $c, produce: $u) isa transaction");
        Concludable q = concludable("$q(buyer: $s, seller: $t, produce: $v) isa transaction");
        Map<String, String> varNameMap = singleAlphaMap(r, q);
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

    @Test
    public void test_relation_annotated_with_attribute() {
        schema("define annotation sub attribute, value string; " +
                       "data-lineage sub relation, relates data, relates source, owns annotation;" +
                       "data sub entity, plays data-lineage:source, plays data-lineage:data;" +
                       "employment sub relation, relates employer, relates employee; person sub entity, plays employment:employee, plays employment:employer;");
        Concludable r1 = concludable("$r1 ($x, $y)");
        Concludable r2 = relation(concludable("$r2 ($x, $y)", "$r2 has $a"));
        testAlphaEquivalenceSymmetricReflexive(r1, r2, false);
    }

    @Test
    public void test_relation_with_different_roleplayer_variable_bindings() {
        schema("define employment sub relation, relates employer, relates employee; company sub entity, " +
                       "plays employment:employer, plays employment:employee;");
        List<Concludable> concludables = Iterators.iterate(
                concludable("$r1 (employer: $x, employee: $y)"),
                concludable("$r2 (employer: $x, employee: $x)"),
                concludable("$r3 (employer: $x, employee: $y, employee: $z)"),
                concludable("$r4 (employer: $x, employee: $x, employee: $y)")
        ).toList();
        concludables.forEach(c -> testAlphaEquivalenceSymmetricReflexive(c, concludables, new HashSet<>()));
    }

    @Test
    public void test_relation_with_typed_roleplayers() {
        schema("define employment sub relation, relates employer, relates employee; organisation sub entity, plays " +
                       "employment:employer, plays employment:employee; company sub organisation;");
        List<Concludable> concludables = Iterators.iterate(
                relation(concludable("($x, $y)", "$x isa organisation")),
                relation(concludable("($x, $y)", "$y isa organisation")),
                relation(concludable("$r0 ($x, $y)", "$x isa organisation")),
                relation(concludable("$r1 ($x, $y)", "$y isa organisation")),
                relation(concludable("($x, $y)", "$x isa company")),
                relation(concludable("($x, $y)", "$y isa organisation", "$x isa organisation")),
                concludable("(employer: $x, employee: $y)"),
                relation(concludable("(employer: $x, employee: $y)", "$x isa organisation")),
                relation(concludable("(employer: $x, employee: $y)", "$y isa organisation")),
                relation(concludable("(employer: $x, employee: $y)", "$x isa organisation", "$y isa company")),
                relation(concludable("(employer: $x, employee: $y)", "$x isa organisation", "$y isa organisation")),
                relation(concludable("$r2 ($x, $y)", "$x isa company")),
                relation(concludable("$r3 ($x, $y)", "$y isa organisation", "$x isa organisation")),
                concludable("$r4 (employer: $x, employee: $y)"),
                relation(concludable("$r5 (employer: $x, employee: $y)", "$x isa organisation")),
                relation(concludable("$r6 (employer: $x, employee: $y)", "$y isa organisation")),
                relation(concludable("$r7 (employer: $x, employee: $y)", "$x isa organisation", "$y isa company")),
                relation(concludable("$r8 (employer: $x, employee: $y)", "$x isa organisation", "$y isa organisation")))
                .toList();
        testAlphaEquivalenceSymmetricReflexive(concludables.get(0), concludables, set(1));
        testAlphaEquivalenceSymmetricReflexive(concludables.get(1), concludables, set(0));
        testAlphaEquivalenceSymmetricReflexive(concludables.get(2), concludables, set(3));
        testAlphaEquivalenceSymmetricReflexive(concludables.get(3), concludables, set(2));
        for (int vari = 4; vari < concludables.size(); vari++)
            testAlphaEquivalenceSymmetricReflexive(concludables.get(vari), concludables, new HashSet<>());
    }

    @Test
    public void test_relation_attributes_as_roleplayers() {
        schema("define name sub attribute, value string, plays signature:name; signature sub relation, relates name, " +
                       "relates form; paper sub entity, plays signature:form;");
        List<Concludable> concludables = Iterators.iterate(
                concludable("$r1 ($x, $y)"),
                relation(concludable("$r2 ($x, $y)", "$x isa name")),
                relation(concludable("$r3 ($x, $y)", "$x 'Bob' isa name")),
                relation(concludable("$r4 ($x, $y)", "$y 'Bob' isa name")),
                relation(concludable("$r5 ($x, $y)", "$y 'Alice' isa name")),
                relation(concludable("$r6 ($x, $y)", "$x 'Bob' isa name", "$y 'Alice' isa name")),
                relation(concludable("$r7 ($x, $y)", "$y 'Bob' isa name", "$x 'Alice' isa name"))
        ).toList();
        testAlphaEquivalenceSymmetricReflexive(concludables.get(0), concludables, new HashSet<>());
        testAlphaEquivalenceSymmetricReflexive(concludables.get(1), concludables, new HashSet<>());
        testAlphaEquivalenceSymmetricReflexive(concludables.get(2), concludables, set(3));
        testAlphaEquivalenceSymmetricReflexive(concludables.get(3), concludables, set(2));
        testAlphaEquivalenceSymmetricReflexive(concludables.get(4), concludables, new HashSet<>());
        testAlphaEquivalenceSymmetricReflexive(concludables.get(5), concludables, set(6));
        testAlphaEquivalenceSymmetricReflexive(concludables.get(6), concludables, set(5));
    }

    @Test
    public void test_relation_different_inequivalent_variants() {
        schema("define parentship sub relation, relates parent, relates child; " +
                       "person sub entity, plays parentship:parent, plays parentship:child;");
        List<Concludable> concludables = Iterators.iterate(
                concludable("($y)"),
                concludable("($y) isa $type"),
                relation(concludable("($y) isa $type", "$type type parentship")),
                concludable("($y) isa parentship"),
                concludable("(parent: $y) isa parentship"),
                relation(concludable("(parent: $y) isa $type", "$type type parentship")),
                concludable("($x, $y)"),
                concludable("($x, $y) isa $type"),
                concludable("($x, $y) isa parentship"),
                relation(concludable("($x, $y) isa $type", "$type type parentship")),
                concludable("($y, child: $z)"),
                concludable("($y, child: $z) isa $type"),
                concludable("($y, child: $z) isa parentship"),
                relation(concludable("($y, child: $z) isa $type", "$type type parentship")),
                concludable("(parent: $x, child: $y)"),
                concludable("(parent: $x, child: $y) isa $type"),
                concludable("(parent: $x, child: $y) isa parentship"),
                relation(concludable("(parent: $x, child: $y) isa $type", "$type type parentship")),
                concludable("(parent: $x, child: $y, $z) isa parentship"),
                concludable("$r0 ($y)"),
                concludable("$r1 ($y) isa $type"),
                concludable("$r2 (parent: $y) isa parentship"),
                relation(concludable("$r3 (parent: $y) isa $type", "$type type parentship")),
                concludable("$r4 (parent: $y, child: $z) isa parentship"),
                relation(concludable("$r5 (parent: $y, child: $z) isa $pship", "$pship type parentship")),
                concludable("$r6 ($y, $z) isa parentship"),
                concludable("$r7 (parent: $x, child: $y, $z) isa parentship"),
                concludable("$r8 ($y, $z, $u) isa parentship")
        ).toList();
        concludables.forEach(c -> testAlphaEquivalenceSymmetricReflexive(c, concludables, new HashSet<>()));
    }

    private static void testAlphaEquivalenceSymmetricReflexive(Concludable concludableA, Concludable concludableB,
                                                               boolean isValid) {
        assertTrue(concludableA.alphaEquals(concludableA).first().isPresent());
        assertTrue(concludableB.alphaEquals(concludableB).first().isPresent());
        assertEquals("Variable:\n" + concludableA + "\n=?\n" + concludableB, isValid, concludableA.alphaEquals(concludableB).first().isPresent());
        assertEquals("Variable:\n" + concludableB + "\n=?\n" + concludableA, isValid, concludableB.alphaEquals(concludableA).first().isPresent());
    }

    private static void testAlphaEquivalenceSymmetricReflexive(Concludable source, List<Concludable> toCheck, Set<Integer> validVars) {
        //use var index as .equals on Variable doesn't seem reliable
        for (int concludableIndex = 0; concludableIndex < toCheck.size(); concludableIndex++) {
            Concludable concludable = toCheck.get(concludableIndex);
            //compare only by reference
            if (concludable != source) {
                testAlphaEquivalenceSymmetricReflexive(source, concludable, validVars.contains(concludableIndex));
            }
        }
    }
}
