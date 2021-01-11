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

package grakn.core.logic.resolvable;

import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import graql.lang.Graql;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static grakn.common.collection.Collections.set;
import static junit.framework.TestCase.assertEquals;

public class RetrievableTest {

    private Conjunction parse(String query) {
        return Disjunction.create(Graql.parsePattern(query).asConjunction().normalise()).conjunctions().iterator().next();
    }

    @Test
    public void test_has_concludable_splits_retrievables() {
        Set<Concludable<?>> concludables = Concludable.create(parse("{ $p has $n; }"));
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse("{ $p isa person; $p has $n; $n isa name; }"), concludables);
        assertEquals(1, concludables.size());
        assertEquals(set(parse("{ $p isa person; }"), parse("{ $n isa name; }")),
                     retrievables.stream().map(Retrievable::conjunction).collect(Collectors.toSet()));
    }

    @Test
    public void test_isa_concludable_omitted_from_retrievable() {
        Set<Concludable<?>> concludables = Concludable.create(parse("{ $p isa person; }"));
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse("{ $p isa person; $p has $n; $n isa name; }"), concludables);
        assertEquals(1, concludables.size());
        assertEquals(set(parse("{ $p has $n; $n isa name; }")),
                     retrievables.stream().map(Retrievable::conjunction).collect(Collectors.toSet()));
    }

    @Test
    public void test_lone_type_variable_is_not_a_retrievable() {
        Set<Concludable<?>> concludables = Concludable.create(parse("{ $p isa $pt; $pt type person; }"));
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse(
                "{ $p isa $pt; $pt type person; $p has $n; $n isa name; }"), concludables);
        assertEquals(1, concludables.size());
        assertEquals(set(parse("{ $p has $n; $n isa name; }"),
                         parse("{ $pt type person; }")),
                     retrievables.stream().map(Retrievable::conjunction).collect(Collectors.toSet()));
    }

    @Test
    public void test_isa_named_type_var_sub_is_a_retrievable() {
        Set<Concludable<?>> concludables = Concludable.create(parse("{ $p isa $pt; $pt sub person; }"));
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse(
                "{ $p isa $pt; $pt sub person; $p has $n; $n isa name; }"), concludables);
        assertEquals(1, concludables.size());
        assertEquals(set(parse("{ $p has $n; $n isa name; }"), parse("{ $pt sub person; }")),
                     retrievables.stream().map(Retrievable::conjunction).collect(Collectors.toSet()));
    }

    @Test
    public void test_relation_concludable_excluded_from_retrievable() {
        Set<Concludable<?>> concludables = Concludable.create(parse("{ $e(employee: $p, employer:$c) isa employment; }"));
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse(
                "{ $e(employee: $p, employer:$c) isa employment; $p isa person, has name \"Alice\"; $c isa company; }"), concludables);
        assertEquals(1, concludables.size());
        assertEquals(set(parse("{ $p isa person, has name \"Alice\"; }"),
                         parse("{ $c isa company; }")),
                     retrievables.stream().map(Retrievable::conjunction).collect(Collectors.toSet()));
    }

    @Test
    public void test_relation_and_isas_excluded_from_retrievable() {
        Set<Concludable<?>> concludables = Concludable.create(parse(
                "{ $e(employee: $p, employer:$c) isa employment; $p isa person; $c isa company; }"));
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse(
                "{ $e(employee: $p, employer:$c) isa employment; $p isa person, has name \"Alice\"; $c isa company; }"), concludables);
        assertEquals(3, concludables.size());
        assertEquals(set(parse("{ $p has name \"Alice\"; }")),
                     retrievables.stream().map(Retrievable::conjunction).collect(Collectors.toSet()));
    }

    @Test
    public void test_relation_is_in_retrievable() {
        Set<Concludable<?>> concludables = Concludable.create(parse("{ $p isa person, has name $n; $n \"Alice\"; }"));
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse(
                "{ $e(employee: $p, employer:$c) isa employment; $p isa person, has name $n; $n \"Alice\"; $c isa company; }"), concludables);
        assertEquals(2, concludables.size());
        assertEquals(set(parse("{ $e(employee: $p, employer:$c) isa employment; $c isa company; }")),
                     retrievables.stream().map(Retrievable::conjunction).collect(Collectors.toSet()));
    }

    @Test
    public void test_termination_building_retrievable_with_a_has_cycle() {
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse("{ $a has $b; $b has $a; }"), set());
        assertEquals(set(parse("{ $a has $b; $b has $a; }")),
                     retrievables.stream().map(Retrievable::conjunction).collect(Collectors.toSet()));
    }

    @Test
    public void test_termination_building_retrievable_with_a_relation_cycle() {
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse("{ $a($b); $b($a); }"), set());
        assertEquals(set(parse("{ $a($b); $b($a); }")),
                     retrievables.stream().map(Retrievable::conjunction).collect(Collectors.toSet()));
    }

    @Test
    public void test_multiple_value_constraints_in_retrievable() {
        Set<Concludable<?>> concludables = Concludable.create(parse("{ $a has $b; }"));
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse("{ $a has $b; $b > 5; $b < 10; }"), concludables);
        assertEquals(1, concludables.size());
        assertEquals(set(parse("{ $b > 5; $b < 10; }")),
                     retrievables.stream().map(Retrievable::conjunction).collect(Collectors.toSet()));
    }

    @Test
    public void test_disconnected_conjunction_in_retrievable() {
        Set<Retrievable> retrievables = Retrievable.extractFrom(parse("{ $a = 7; $b > 5; $b < 10; }"), set());
        assertEquals(set(parse("{ $b > 5; $b < 10; }"),
                         parse("{ $a = 7; }")),
                     retrievables.stream().map(Retrievable::conjunction).collect(Collectors.toSet()));
    }
}
