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

import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typeql.lang.TypeQL;
import org.junit.Test;

import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class RetrievableTest {

    private Conjunction parse(String query) {
        return Disjunction.create(TypeQL.parsePattern(query).asConjunction().normalise()).conjunctions().iterator().next();
    }

    @Test
    public void test_has_concludable_splits_retrievables() {
        Set<Concludable> concludables = Concludable.create(parse("{ $p has $n; }"));
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse("{ $p isa person; $p has $n; $n isa name; }"), concludables);
        assertEquals(1, concludables.size());
        assertEquals(set(parse("{ $p isa person; }"), parse("{ $n isa name; }")),
                     iterate(retrievables).map(Retrievable::pattern).toSet());
    }

    @Test
    public void test_isa_concludable_omitted_from_retrievable() {
        Set<Concludable> concludables = Concludable.create(parse("{ $p isa person; }"));
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse("{ $p isa person; $p has $n; $n isa name; }"), concludables);
        assertEquals(1, concludables.size());
        assertEquals(set(parse("{ $p has $n; $n isa name; }")),
                     iterate(retrievables).map(Retrievable::pattern).toSet());
    }

    @Test
    public void test_lone_type_variable_is_not_a_retrievable() {
        Set<Concludable> concludables = Concludable.create(parse("{ $p isa $pt; $pt type person; }"));
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse(
                "{ $p isa $pt; $pt type person; $p has $n; $n isa name; }"), concludables);
        assertEquals(1, concludables.size());
        assertEquals(set(parse("{ $p has $n; $n isa name; }"),
                         parse("{ $pt type person; }")),
                     iterate(retrievables).map(Retrievable::pattern).toSet());
    }

    @Test
    public void test_isa_named_type_var_sub_is_a_retrievable() {
        Set<Concludable> concludables = Concludable.create(parse("{ $p isa $pt; $pt sub person; }"));
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse(
                "{ $p isa $pt; $pt sub person; $p has $n; $n isa name; }"), concludables);
        assertEquals(1, concludables.size());
        assertEquals(set(parse("{ $p has $n; $n isa name; }"), parse("{ $pt sub person; }")),
                     iterate(retrievables).map(Retrievable::pattern).toSet());
    }

    @Test
    public void test_relation_concludable_excluded_from_retrievable() {
        Set<Concludable> concludables = Concludable.create(parse("{ $e(employee: $p, employer:$c) isa employment; }"));
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse(
                "{ $e(employee: $p, employer:$c) isa employment; $p isa person, has name \"Alice\"; $c isa company; }"), concludables);
        assertEquals(1, concludables.size());
        assertEquals(set(parse("{ $p isa person, has name \"Alice\"; }"),
                         parse("{ $c isa company; }")),
                     iterate(retrievables).map(Retrievable::pattern).toSet());
    }

    @Test
    public void test_relation_and_isas_excluded_from_retrievable() {
        Set<Concludable> concludables = Concludable.create(parse(
                "{ $e(employee: $p, employer:$c) isa employment; $p isa person; $c isa company; }"));
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse(
                "{ $e(employee: $p, employer:$c) isa employment; $p isa person, has name \"Alice\"; $c isa company; }"), concludables);
        assertEquals(3, concludables.size());
        assertEquals(set(parse("{ $p has name \"Alice\"; }")),
                     iterate(retrievables).map(Retrievable::pattern).toSet());
    }

    @Test
    public void test_shared_role_between_concludable_and_retrievable() {
        Set<Concludable> concludables = Concludable.create(parse(
                "{ $e(employee: $p, employer:$c) isa employment; $p isa person; $c isa company; }"));
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse(
                "{ $e(employee: $p, employer:$c) isa employment; $r (employee: $p) isa employment; $p isa non-inferred-role-player; }"), concludables);
        assertEquals(3, concludables.size());
        assertEquals(set(parse("{ $r (employee: $p) isa employment; $p isa non-inferred-role-player; }")),
                     iterate(retrievables).map(Retrievable::pattern).toSet());
    }

    @Test
    public void test_relation_is_in_retrievable() {
        Set<Concludable> concludables = Concludable.create(parse("{ $p isa person, has name $n; $n \"Alice\"; }"));
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse(
                "{ $e(employee: $p, employer:$c) isa employment; $p isa person, has name $n; $n \"Alice\"; $c isa company; }"), concludables);
        assertEquals(2, concludables.size());
        assertEquals(set(parse("{ $e(employee: $p, employer:$c) isa employment; $c isa company; }")),
                     iterate(retrievables).map(Retrievable::pattern).toSet());
    }

    @Test
    public void test_termination_building_retrievable_with_a_has_cycle() {
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse("{ $a has $b; $b has $a; }"), set());
        assertEquals(set(parse("{ $a has $b; $b has $a; }")),
                     iterate(retrievables).map(Retrievable::pattern).toSet());
    }

    @Test
    public void test_termination_building_retrievable_with_a_relation_cycle() {
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse("{ $a($role1: $b); $b($role2: $a); }"), set());
        assertEquals(set(parse("{ $a($role1: $b); $b($role2: $a); }")),
                     iterate(retrievables).map(Retrievable::pattern).toSet());
    }

    @Test
    public void test_multiple_value_constraints_in_retrievable() {
        Set<Concludable> concludables = Concludable.create(parse("{ $a has $b; }"));
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse("{ $a has $b; $b > 5; $b < 10; }"), concludables);
        assertEquals(1, concludables.size());
        assertEquals(set(parse("{ $b > 5; $b < 10; }")),
                     iterate(retrievables).map(Retrievable::pattern).toSet());
    }

    @Test
    public void test_only_disconnected_retrievables_are_separated() {
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse("{ $a = 7; $b > 5; $b < 10; }"), set());
        assertEquals(set(parse("{ $b > 5; $b < 10; }"),
                         parse("{ $a = 7; }")),
                     iterate(retrievables).map(Retrievable::pattern).toSet());
    }

    @Test
    public void test_retrievables_split_by_labelled_type_are_separated() {
        Set<Retrievable> retrievables = Retrievable.extractFrom(
                parse("{ $r1 (employer: $c1, employee: $p1) isa employment; $r2 (employer: $c2, employee: $p2) isa employment; }"),
                set()
        );
        assertEquals(2, retrievables.size());
    }

    @Test
    public void test_has_value_constraints_are_in_retrievable_and_concludable() {
        Set<Concludable> concludables = Concludable.create(parse("{ $a has $b; $b > 5; $b < 10; }"));
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse("{ $a has $b; $b > 5; $b < 10; }"), concludables);
        assertEquals(set(parse("{ $b > 5; $b < 10; }")),
                     iterate(retrievables).map(Retrievable::pattern).toSet());
    }

    @Test
    public void test_has_with_anonymous_attribute_value_constraints_are_in_retrievable_and_concludable() {
        Set<Concludable> concludables = Concludable.create(parse("{ $x has age 30; }"));
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse("{ $x has age 30; }"), concludables);
        assertEquals(set(), iterate(retrievables).map(Retrievable::pattern).toSet());
    }

    @Test
    public void test_isa_value_constraints_are_in_retrievable_and_concludable() {
        Set<Concludable> concludables = Concludable.create(parse("{ $a isa $b; $a > 5; $a < 10; }"));
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse("{ $a isa $b; $a > 5; $a < 10; }"), concludables);
        assertEquals(set(parse("{ $a > 5; $a < 10; }")),
                     iterate(retrievables).map(Retrievable::pattern).toSet());
    }

    @Test
    public void test_attribute_value_constraints_are_in_retrievable_and_concludable() {
        Set<Concludable> concludables = Concludable.create(parse("{ $a > 5; $a < 10; }"));
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse("{ $a > 5; $a < 10; }"), concludables);
        assertEquals(set(parse("{ $a > 5; $a < 10; }")),
                     iterate(retrievables).map(Retrievable::pattern).toSet());
    }

    @Test
    public void test_variable_value_constraints_create_retrievable_and_two_concludables() {
        Set<Concludable> concludables = Concludable.create(parse("{ $x > $y; }"));
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse("{ $x > $y; }"), concludables);
        assertEquals(set(parse("{ $x > $y; }")),
                     iterate(retrievables).map(Retrievable::pattern).toSet());
    }

    @Test
    public void test_variable_value_equals_constraints_create_retrievable_and_two_concludables() {
        Set<Concludable> concludables = Concludable.create(parse("{ $x = $y; }"));
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse("{ $x = $y; }"), concludables);
        assertEquals(set(parse("{ $x = $y; }")),
                     iterate(retrievables).map(Retrievable::pattern).toSet());
    }

    @Test
    public void test_equals_constraint_only_present_in_concludable() {
        Set<Concludable> concludables = Concludable.create(parse("{ $x has $a; $a = $b; }"));
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse("{ $x has $a; $a = $b; }"), concludables);
        assertEquals(set(parse("{ $a = $b; }")), iterate(retrievables).map(Retrievable::pattern).toSet());
    }

    @Test
    public void test_is_constraint_is_a_retrievable() {
        Set<Concludable> concludables = Concludable.create(parse("{ $x is $y; $x isa thing; $y isa thing; }"));
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse("{ $x is $y; $x isa thing; $y isa thing; }"), concludables);
        // We can't build the conjunction { $x is $y; } using TypeQL (without binding the variables)
        assertEquals(1, retrievables.size());
        assertEquals(2, retrievables.iterator().next().pattern().variables().size());
        assertTrue(retrievables.iterator().next().pattern().variables().stream().filter(
                v -> v.reference().asName().name().equals("x")).findFirst().get().constraints().iterator().next().asThing().isIs());
    }
}
