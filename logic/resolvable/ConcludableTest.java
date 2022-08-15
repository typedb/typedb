/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.logic.resolvable;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.pattern.constraint.thing.IsaConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.RelationConstraint;
import com.vaticle.typeql.lang.TypeQL;
import org.junit.Test;

import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static junit.framework.Assert.assertFalse;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class ConcludableTest {

    private Conjunction parseConjunction(String query) {
        return Disjunction.create(TypeQL.parsePattern(query).asConjunction().normalise()).conjunctions().iterator().next();
    }

    private long relationConcludablesCount(Set<Concludable> concludables) {
        return concludables.stream().filter(Concludable::isRelation).count();
    }

    private long hasConcludablesCount(Set<Concludable> concludables) {
        return concludables.stream().filter(Concludable::isHas).count();
    }

    private long isaConcludablesCount(Set<Concludable> concludables) {
        return concludables.stream().filter(Concludable::isIsa).count();
    }

    private long attributeConcludablesCount(Set<Concludable> concludables) {
        return concludables.stream().filter(Concludable::isAttribute).count();
    }

    @Test
    public void test_conjunction_only_isa_is_built() {
        String conjunction = "{ $a isa b; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(1, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_isa_value_constrained_is_built() {
        String conjunction = "{ $x isa milk; $a 10 isa age-in-days; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(2, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_only_has_is_built() {
        String conjunction = "{ $a has $b; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(1, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_only_has_is_built_when_type_and_value_given() {
        String conjunction = "{ $a has age 50; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(1, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_isa_has_value_constrained_is_built() {
        String conjunction = "{ $x isa milk, has age-in-days >= 10; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(1, isaConcludablesCount(concludables));
        assertEquals(1, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_only_relation_is_built() {
        String conjunction = "{ $a($role1: $x, $role2: $y); }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(1, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_only_relation_is_built_when_type_is_given() {
        String conjunction = "{ $a($role1: $x, $role2: $y) isa siblingship; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(1, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_has_and_relation_are_built_from_same_owner() {
        String conjunction = "{ $a($role1: $x, $role2: $y) isa siblingship, has number 2; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(1, hasConcludablesCount(concludables));
        assertEquals(1, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_only_isa_is_built_when_isa_owner_has_value() {
        String conjunction = "{ $a 5; $a isa age; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(1, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_only_value_is_built() {
        String conjunction = "{ $a 5; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(1, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_multiple_has_and_relation_are_built() {
        String conjunction = "{ $a($role1: $x, $role2: $y, $role3: $z) isa transaction, has currency \"GBP\", has value 45; " +
                "$b(locates: $a); $b has $c; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(3, hasConcludablesCount(concludables));
        assertEquals(2, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_isa_and_has_are_built() {
        String conjunction = "{ $p isa person, has name $n; $n \"Alice\"; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(1, isaConcludablesCount(concludables));
        assertEquals(1, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_only_creates_has_concludable_when_attribute_has_value_constraints() {
        String conjunction = "{ $x has $a; $a isa age; $a > 5; $a <= 10; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(1, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_creates_multiple_value_constraints_for_same_owned_attribute_without_an_isa() {
        String conjunction = "{ $x has $a; $a > 5; $a <= 10; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(1, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_creates_multiple_value_constraints_for_same_attribute() {
        String conjunction = "{ $a > 5; $a <= 10; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(1, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_creates_one_has_one_isa_concludable_for_attribute_owning_attribute() {
        String conjunction = "{ $a has $b; $a \"Hi there\" isa content; $b \"English\" isa language; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(1, isaConcludablesCount(concludables));
        assertEquals(1, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_creates_two_attribute_concludables_for_variable_value_comparison() {
        String conjunction = "{ $x > $y; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(2, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_creates_two_attribute_concludables_for_variable_and_constant_value_comparison() {
        String conjunction = "{ $x > $y; $y = 5; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(2, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_creates_attribute_and_isa_concludables_for_variable_value_comparison() {
        String conjunction = "{ $x > $y; $y isa age; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(1, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(1, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_concludables_does_not_contain_type_labels() {
        String conjunction = "{ $diag(patient-role-type: $per) isa $diagnosis; $diagnosis type diagnosis-type;" +
                "$per isa $person; $person type person-type; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(1, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(1, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
        RelationConstraint relationConstraint = concludables.stream().filter(Concludable::isRelation).findFirst().get()
                .asRelation().relation();
        IsaConstraint relationIsaConstraint = relationConstraint.owner().isa().get();
        assertEquals("$diagnosis", relationIsaConstraint.type().reference().asName().syntax());
        assertFalse(relationIsaConstraint.type().label().isPresent());
        assertFalse(list(relationConstraint.players()).get(0).player().isa().isPresent());
    }

    @Test
    public void test_conjunction_concludables_contain_type_labels_with_anonymous_type_variable() {
        String conjunction = "{ $diag(patient-role-type: $per) isa diagnosis; $per isa person; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(1, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(1, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
        RelationConstraint relationConstraint = concludables.stream().filter(Concludable::isRelation).findFirst()
                .get().asRelation().relation();
        IsaConstraint relationIsaConstraint = relationConstraint.owner().isa().get();
        assertEquals("$_diagnosis", relationIsaConstraint.type().reference().asLabel().syntax());
        assertEquals("diagnosis", relationIsaConstraint.type().label().get().label());
        assertFalse(list(relationConstraint.players()).get(0).player().isa().isPresent());
    }

    @Test
    public void test_conjunction_concludables_contain_type_labels_with_anonymous_type_and_relation_variable() {
        String conjunction = "{ (patient-role-type: $per) isa diagnosis; $per isa person; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(1, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(1, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
        RelationConstraint relationConstraint = concludables.stream().filter(Concludable::isRelation).findFirst().get()
                .asRelation().relation();
        IsaConstraint relationIsaConstraint = relationConstraint.owner().isa().get();
        assertEquals("$_diagnosis", relationIsaConstraint.type().reference().asLabel().syntax());
        assertEquals("diagnosis", relationIsaConstraint.type().label().get().label());
        assertFalse(list(relationConstraint.players()).get(0).player().isa().isPresent());
    }

    @Test
    public void test_conjunction_anonymous_variable_constraints_are_not_conflated() {
        String conjunction = "{ (patient-role-type: $per) isa diagnosis; (patient-role-type: $p2) isa diagnosis; " +
                "$per isa person; $per has age 50; $per has age 70; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(1, isaConcludablesCount(concludables));
        assertEquals(2, hasConcludablesCount(concludables));
        assertEquals(2, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
        concludables.stream().filter(Concludable::isRelation).map(Concludable::asRelation).forEach(relationConcludable -> {
            assertTrue(relationConcludable.relation().owner().isa().isPresent());
        });
        concludables.stream().filter(Concludable::isHas).map(Concludable::asHas).forEach(hasConcludable -> {
            assertEquals(1, hasConcludable.has().attribute().value().size());
            assertTrue(hasConcludable.has().attribute().isa().isPresent());
        });
    }

    @Test
    public void test_comparison_constraint_on_variables_always_unify(){
        Pair<String,String>[] concludableConclusionPairs = new Pair[]{
            // Both var
            new Pair("{ $x has attr $v; }", "{$y has attr $w;}"),
            new Pair("{ $x has attr = $v; }", "{$y has attr $w;}"),
            new Pair("{ $x has attr != $v; }", "{$y has attr $w;}"),
            new Pair("{ $x has attr <= $v; }", "{$y has attr $w;}"),
            new Pair("{ $x has attr >= $v; }", "{$y has attr $w;}"),
            new Pair("{ $x has attr < $v; }", "{$y has attr $w;}"),
            new Pair("{ $x has attr > $v; }", "{$y has attr $w;}"),

            // Boolean
            new Pair("{ $x has attr $v; }", "{$y has attr true;}"),
            new Pair("{ $x has attr true; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr $v; }", "{$y has attr false;}"),
            new Pair("{ $x has attr false; }", "{$y has attr $v;}"),

            new Pair("{ $x has attr = $v; }", "{$y has attr true;}"),
            new Pair("{ $x has attr = true; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr = $v; }", "{$y has attr false;}"),
            new Pair("{ $x has attr = false; }", "{$y has attr $v;}"),

            new Pair("{ $x has attr != $v; }", "{$y has attr true;}"),
            new Pair("{ $x has attr != true; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr != $v; }", "{$y has attr false;}"),
            new Pair("{ $x has attr != false; }", "{$y has attr $v;}"),

            // Numeric assign, equality, gte, lte
            new Pair("{ $x has attr 1; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr $v; }", "{$y has attr 1;}"),
            new Pair("{ $x has attr 1.0; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr $v; }", "{$y has attr 1.0;}"),

            new Pair("{ $x has attr = 1; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr = $v; }", "{$y has attr 1;}"),
            new Pair("{ $x has attr = 1.0; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr = $v; }", "{$y has attr 1.0;}"),

            new Pair("{ $x has attr >= 1; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr >= $v; }", "{$y has attr 1;}"),
            new Pair("{ $x has attr >= 1.0; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr >= $v; }", "{$y has attr 1.0;}"),

            new Pair("{ $x has attr <= 1; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr <= $v; }", "{$y has attr 1;}"),
            new Pair("{ $x has attr <= 1.0; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr <= $v; }", "{$y has attr 1.0;}"),

            // Numeric inequality, gt, lt
            new Pair("{ $x has attr != 1; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr != $v; }", "{$y has attr 1;}"),
            new Pair("{ $x has attr != 1.0; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr != $v; }", "{$y has attr 1.0;}"),

            new Pair("{ $x has attr > 1; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr > $v; }", "{$y has attr 1;}"),
            new Pair("{ $x has attr > 1.0; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr > $v; }", "{$y has attr 1.0;}"),

            new Pair("{ $x has attr < 1; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr < $v; }", "{$y has attr 1;}"),
            new Pair("{ $x has attr < 1.0; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr < $v; }", "{$y has attr 1.0;}"),

            // String comparisons
            new Pair("{ $x has attr \"one\"; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr = $v; }", "{$y has attr \"one\";}"),
            new Pair("{ $x has attr >= \"one\"; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr >= $v; }", "{$y has attr \"one\";}"),
            new Pair("{ $x has attr <= \"one\"; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr <= $v; }", "{$y has attr \"one\";}"),
            new Pair("{ $x has attr != $v; }", "{$y has attr \"one\";}"),
            new Pair("{ $x has attr != \"one\"; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr > \"one\"; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr > $v; }", "{$y has attr \"one\";}"),
            new Pair("{ $x has attr < \"one\"; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr < $v; }", "{$y has attr \"one\";}"),

            // DateTime
            new Pair("{ $x has attr 2022-01-01; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr $v; }", "{$y has attr 2022-01-01;}"),
            new Pair("{ $x has attr = 2022-01-01; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr = $v; }", "{$y has attr 2022-01-01;}"),
            new Pair("{ $x has attr != 2022-01-01; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr != $v; }", "{$y has attr 2022-01-01;}"),
            new Pair("{ $x has attr >= 2022-01-01; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr >= $v; }", "{$y has attr 2022-01-01;}"),
            new Pair("{ $x has attr <= 2022-01-01; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr <= $v; }", "{$y has attr 2022-01-01;}"),
            new Pair("{ $x has attr > 2022-01-01; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr > $v; }", "{$y has attr 2022-01-01;}"),
            new Pair("{ $x has attr < 2022-01-01; }", "{$y has attr $v;}"),
            new Pair("{ $x has attr < $v; }", "{$y has attr 2022-01-01;}"),
        };

        for (Pair<String, String> pair : concludableConclusionPairs) {
            Concludable concludable = iterate(Concludable.create(parseConjunction(pair.first()))).next();
            Concludable conclusion = iterate(Concludable.create(parseConjunction(pair.second()))).next();

            assertTrue(concludable.isHas());
            assertTrue(conclusion.isHas());
            assertTrue(concludable.unificationSatisfiable(concludable.asHas().attribute(), conclusion.asHas().attribute()));
        }
    }

    @Test
    public void test_satisfiable_comparison_constraints_unify() {
        Pair<String, String>[] concludableConclusionPairs = new Pair[]{
            // Boolean
            new Pair("{ $x true; }", "{$y true;}"),
            new Pair("{ $x false; }", "{$y false;}"),
            new Pair("{ $x = true; }", "{$y true;}"),
            new Pair("{ $x = false; }", "{$y false;}"),
            new Pair("{ $x != true; }", "{$y false;}"),
            new Pair("{ $x != false; }", "{$y true;}"),

            // Numeric assign, equality, gte, lte
            new Pair("{ $x 1; }", "{$y 1;}"),
            new Pair("{ $x 1; }", "{$y 1.0;}"),
            new Pair("{ $x 1.0; }", "{$y 1;}"),
            new Pair("{ $x 1.0; }", "{$y 1.0;}"),

            new Pair("{ $x = 1; }", "{$y 1;}"),
            new Pair("{ $x = 1; }", "{$y 1.0;}"),
            new Pair("{ $x = 1.0; }", "{$y 1;}"),
            new Pair("{ $x = 1.0; }", "{$y 1.0;}"),

            new Pair("{ $x >= 1; }", "{$y 1;}"),
            new Pair("{ $x >= 1; }", "{$y 1.0;}"),
            new Pair("{ $x >= 1.0; }", "{$y 1;}"),
            new Pair("{ $x >= 1.0; }", "{$y 1.0;}"),

            new Pair("{ $x <= 1; }", "{$y 1;}"),
            new Pair("{ $x <= 1; }", "{$y 1.0;}"),
            new Pair("{ $x <= 1.0; }", "{$y 1;}"),
            new Pair("{ $x <= 1.0; }", "{$y 1.0;}"),

            // Numeric inequality, gt, lt
            new Pair("{ $x != 1; }", "{$y 2;}"),
            new Pair("{ $x != 1; }", "{$y 2.0;}"),
            new Pair("{ $x != 1.0; }", "{$y 2;}"),
            new Pair("{ $x != 1.0; }", "{$y 2.0;}"),

            new Pair("{ $x > 1; }", "{$y 2;}"),
            new Pair("{ $x > 1; }", "{$y 2.0;}"),
            new Pair("{ $x > 1.0; }", "{$y 2;}"),
            new Pair("{ $x > 1.0; }", "{$y 2.0;}"),

            new Pair("{ $x < 1; }", "{$y -2;}"),
            new Pair("{ $x < 1; }", "{$y -2.0;}"),
            new Pair("{ $x < 1.0; }", "{$y -2;}"),
            new Pair("{ $x < 1.0; }", "{$y -2.0;}"),

            // String comparisons
            new Pair("{ $x \"one\"; }", "{$y \"one\";}"),
            new Pair("{ $x = \"one\"; }", "{$y \"one\";}"),
            new Pair("{ $x >= \"one\"; }", "{$y \"one\";}"),
            new Pair("{ $x <= \"one\"; }", "{$y \"one\";}"),
            new Pair("{ $x != \"one\"; }", "{$y \"two\";}"),
            new Pair("{ $x > \"one\"; }", "{$y \"two\";}"),
            new Pair("{ $x < \"two\"; }", "{$y \"one\";}"),

            // String comparisons, case-sensitivity
            new Pair("{ $x != \"one\"; }", "{$y \"ONE\";}"),
            new Pair("{ $x > \"TWO\"; }", "{$y \"one\";}"),
            new Pair("{ $x < \"one\"; }", "{$y \"TWO\";}"),

            // DateTime
            new Pair("{ $x 2022-01-01; }", "{$y 2022-01-01;}"),
            new Pair("{ $x = 2022-01-01; }", "{$y  2022-01-01;}"),
            new Pair("{ $x != 2022-01-01; }", "{$y  2022-02-02;}"),

            new Pair("{ $x >= 2022-01-01; }", "{$y 2022-01-01;}"),
            new Pair("{ $x >= 2022-01-01; }", "{$y 2022-02-02;}"),
            new Pair("{ $x <= 2022-01-01; }", "{$y 2022-01-01;}"),
            new Pair("{ $x <= 2022-02-02; }", "{$y 2022-01-01;}"),

            new Pair("{ $x > 2022-01-01; }", "{$y 2022-02-02;}"),
            new Pair("{ $x < 2022-02-02; }", "{$y 2022-01-01;}"),
        };

        for (Pair<String, String> pair : concludableConclusionPairs){
            Concludable concludable = iterate(Concludable.create(parseConjunction(pair.first()))).next();
            Concludable conclusion = iterate(Concludable.create(parseConjunction(pair.second()))).next();

            assertTrue(concludable.isAttribute());
            assertTrue(conclusion.isAttribute());
            assertTrue(concludable.unificationSatisfiable(concludable.asAttribute().attribute(), conclusion.asAttribute().attribute()));
        }
    }

    @Test
    public void test_unsatisfiable_comparison_constraints_dont_unify(){
        Pair<String,String>[] concludableConclusionPairs = new Pair[]{
            // Boolean
            new Pair("{ $x true; }", "{$y false;}"),
            new Pair("{ $x false; }", "{$y true;}"),
            new Pair("{ $x = true; }", "{$y false;}"),
            new Pair("{ $x = false; }", "{$y true;}"),
            new Pair("{ $x != true; }", "{$y true;}"),
            new Pair("{ $x != false; }", "{$y false;}"),

            // Numeric assign, equality, gte, lte
            new Pair("{ $x 1; }", "{$y 2;}"),
            new Pair("{ $x 1; }", "{$y 2.0;}"),
            new Pair("{ $x 1.0; }", "{$y 2;}"),
            new Pair("{ $x 1.0; }", "{$y 2.0;}"),

            new Pair("{ $x = 1; }", "{$y 2;}"),
            new Pair("{ $x = 1; }", "{$y 2.0;}"),
            new Pair("{ $x = 1.0; }", "{$y 2;}"),
            new Pair("{ $x = 1.0; }", "{$y 2.0;}"),

            new Pair("{ $x = 1; }", "{$y -1;}"),
            new Pair("{ $x = 1; }", "{$y -1.0;}"),
            new Pair("{ $x = 1.0; }", "{$y -1;}"),
            new Pair("{ $x = 1.0; }", "{$y -1.0;}"),

            new Pair("{ $x >= 1; }", "{$y -1;}"),
            new Pair("{ $x >= 1; }", "{$y -1.0;}"),
            new Pair("{ $x >= 1.0; }", "{$y -1;}"),
            new Pair("{ $x >= 1.0; }", "{$y -1.0;}"),

            new Pair("{ $x <= 1; }", "{$y 2;}"),
            new Pair("{ $x <= 1; }", "{$y 2.0;}"),
            new Pair("{ $x <= 1.0; }", "{$y 2;}"),
            new Pair("{ $x <= 1.0; }", "{$y 2.0;}"),

            // Numeric inequality, gt, lt
            new Pair("{ $x != 1; }", "{$y 1;}"),
            new Pair("{ $x != 1; }", "{$y 1.0;}"),
            new Pair("{ $x != 1.0; }", "{$y 1;}"),
            new Pair("{ $x != 1.0; }", "{$y 1.0;}"),

            new Pair("{ $x > 1; }", "{$y -1;}"),
            new Pair("{ $x > 1; }", "{$y -1.0;}"),
            new Pair("{ $x > 1.0; }", "{$y -1;}"),
            new Pair("{ $x > 1.0; }", "{$y -1.0;}"),

            new Pair("{ $x > 1; }", "{$y 1;}"),
            new Pair("{ $x > 1; }", "{$y 1.0;}"),
            new Pair("{ $x > 1.0; }", "{$y 1;}"),
            new Pair("{ $x > 1.0; }", "{$y 1.0;}"),

            new Pair("{ $x < 1; }", "{$y 1;}"),
            new Pair("{ $x < 1; }", "{$y 1.0;}"),
            new Pair("{ $x < 1.0; }", "{$y 1;}"),
            new Pair("{ $x < 1.0; }", "{$y 1.0;}"),

            new Pair("{ $x < 1; }", "{$y 2;}"),
            new Pair("{ $x < 1; }", "{$y 2.0;}"),
            new Pair("{ $x < 1.0; }", "{$y 2;}"),
            new Pair("{ $x < 1.0; }", "{$y 2.0;}"),

            // String comparisons
            new Pair("{ $x \"one\"; }", "{$y \"two\";}"),
            new Pair("{ $x = \"one\"; }", "{$y \"two\";}"),
            new Pair("{ $x >= \"two\"; }", "{$y \"one\";}"),
            new Pair("{ $x <= \"one\"; }", "{$y \"two\";}"),
            new Pair("{ $x != \"one\"; }", "{$y \"one\";}"),
            new Pair("{ $x > \"one\"; }", "{$y \"one\";}"),
            new Pair("{ $x > \"two\"; }", "{$y \"one\";}"),
            new Pair("{ $x < \"two\"; }", "{$y \"two\";}"),
            new Pair("{ $x < \"one\"; }", "{$y \"two\";}"),

            // String comparisons, case-sensitivity
            new Pair("{ $x \"OnE\"; }", "{$y \"oNe\";}"),
            new Pair("{ $x = \"OnE\"; }", "{$y \"oNe\";}"),
            new Pair("{ $x > \"two\"; }", "{$y \"ONE\";}"),
            new Pair("{ $x < \"ONE\"; }", "{$y \"two\";}"),

            // DateTime
            new Pair("{ $x 2022-01-01; }", "{$y  2022-02-02;}"),
            new Pair("{ $x = 2022-01-01; }", "{$y  2022-02-02;}"),
            new Pair("{ $x != 2022-01-01; }", "{$y  2022-01-01;}"),

            new Pair("{ $x >= 2022-02-02; }", "{$y 2022-01-01;}"),
            new Pair("{ $x <= 2022-01-01; }", "{$y 2022-02-02;}"),

            new Pair("{ $x > 2022-01-01; }", "{$y 2022-01-01;}"),
            new Pair("{ $x > 2022-02-02; }", "{$y 2022-01-01;}"),
            new Pair("{ $x < 2022-01-01; }", "{$y 2022-01-01;}"),
            new Pair("{ $x < 2022-01-01; }", "{$y 2022-02-02;}"),
        };

        for (Pair<String, String> pair : concludableConclusionPairs){
            Concludable concludable = iterate(Concludable.create(parseConjunction(pair.first()))).next();
            Concludable conclusion = iterate(Concludable.create(parseConjunction(pair.second()))).next();

            assertTrue(concludable.isAttribute());
            assertTrue(conclusion.isAttribute());
            assertFalse(concludable.unificationSatisfiable(concludable.asAttribute().attribute(), conclusion.asAttribute().attribute()));
        }
    }

    @Test
    public void test_satisfiable_substring_constraints_unify(){
        Pair<String,String>[] concludableConclusionPairs = new Pair[]{
                // One variable
                new Pair("{ $x has attr like \"\"; }", "{$y has attr $v;}"),
                new Pair("{ $x has attr contains \"jan\"; }", "{$y has attr $v;}"),

                // Both constant
                new Pair("{ $x has attr like \"[0-9]{2}-[a-z]{3}-[0-9]{4}\"; }", "{$y has attr \"01-jan-2022\";}"),
                new Pair("{ $x has attr contains \"jan\"; }", "{$y has attr \"01-jan-2022\";}"),
        };

        for (Pair<String, String> pair : concludableConclusionPairs){
            Concludable concludable = iterate(Concludable.create(parseConjunction(pair.first()))).next();
            Concludable conclusion = iterate(Concludable.create(parseConjunction(pair.second()))).next();

            assertTrue(concludable.isHas());
            assertTrue(conclusion.isHas());
            assertTrue(concludable.unificationSatisfiable(concludable.asHas().attribute(), conclusion.asHas().attribute()));
        }
    }

    @Test
    public void test_unsatisfiable_substring_constraint_dont_unify(){
        Pair<String,String>[] concludableConclusionPairs = new Pair[]{
                // Both constant
                new Pair("{ $x has attr like \"[0-9]{2}-[a-z]{3}-[0-9]{4}\"; }", "{$y has attr \"01-01-2022\";}"),
                new Pair("{ $x has attr contains \"jan\"; }", "{$y has attr \"01-feb-2022\";}"),
        };

        for (Pair<String, String> pair : concludableConclusionPairs){
            Concludable concludable = iterate(Concludable.create(parseConjunction(pair.first()))).next();
            Concludable conclusion = iterate(Concludable.create(parseConjunction(pair.second()))).next();

            assertTrue(concludable.isHas());
            assertTrue(conclusion.isHas());
            assertFalse(concludable.unificationSatisfiable(concludable.asHas().attribute(), conclusion.asHas().attribute()));
        }
    }
}
