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

import java.util.List;
import java.util.Set;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.set;
import static junit.framework.TestCase.assertEquals;

public class ResolvableTest {

    private Conjunction parse(String query) {
        return Disjunction.create(Graql.parsePattern(query).asConjunction().normalise()).conjunctions().iterator().next();
    }

    @Test
    public void test_planner_retrievable_dependent_upon_concludable() {
        Concludable concludable = Concludable.create(parse("{ $a has $b; }")).iterator().next();
        Retrievable retrievable = new Retrievable(parse("{ $c($b); }"));

        Set<Resolvable> resolvables = set(concludable, retrievable);
        List<Resolvable> plan = Resolvable.plan(resolvables);
        assertEquals(list(concludable, retrievable), plan);
    }

    @Test
    public void test_planner_prioritises_retrievable_without_dependencies() {
        Concludable concludable = Concludable.create(parse("{ $p has name $n; }")).iterator().next();
        Retrievable retrievable = new Retrievable(parse("{ $p isa person; }"));

        Set<Resolvable> resolvables = set(concludable, retrievable);

        List<Resolvable> plan = Resolvable.plan(resolvables);
        assertEquals(list(retrievable, concludable), plan);
    }

    @Test
    public void test_planner_prioritises_largest_retrievable_without_dependencies() {
        Retrievable retrievable = new Retrievable(parse("{ $p isa person, has age $a, has first-name $fn, has surname $sn; }"));
        Concludable concludable = Concludable.create(parse("{ ($p, $c); }")).iterator().next();
        Retrievable retrievable2 = new Retrievable(parse("{ $c isa company, has name $cn; }"));

        Set<Resolvable> resolvables = set(retrievable, retrievable2, concludable);

        List<Resolvable> plan = Resolvable.plan(resolvables);
        assertEquals(list(retrievable, concludable, retrievable2), plan);
    }

    @Test
    public void test_planner_starts_at_independent_concludable() {
        Concludable concludable = Concludable.create(parse("{ $r($a, $b); }")).iterator().next();
        Concludable concludable2 = Concludable.create(parse("{ $r has $c; }")).iterator().next();

        Set<Resolvable> resolvables = set(concludable, concludable2);

        List<Resolvable> plan = Resolvable.plan(resolvables);
        assertEquals(list(concludable, concludable2), plan);
    }

    @Test
    public void test_planner_multiple_dependencies() {
        Retrievable retrievable = new Retrievable(parse("{ $p isa person; }"));
        Concludable concludable = Concludable.create(parse("{ $p has name $n; }")).iterator().next();
        Retrievable retrievable2 = new Retrievable(parse("{ $c isa company, has name $n; }"));
        Concludable concludable2 = Concludable.create(parse("{ $e($c, $p2) isa employment; }")).iterator().next();

        Set<Resolvable> resolvables = set(retrievable, retrievable2, concludable, concludable2);
        List<Resolvable> plan = Resolvable.plan(resolvables);

        assertEquals(list(retrievable, concludable, retrievable2, concludable2), plan);
    }

    @Test
    public void test_planner_two_circular_has_dependencies() {
        Concludable concludable = Concludable.create(parse("{ $a has $b; }")).iterator().next();
        Concludable concludable2 = Concludable.create(parse("{ $b has $a; }")).iterator().next();

        Set<Resolvable> resolvables = set(concludable, concludable2);
        List<Resolvable> plan = Resolvable.plan(resolvables);

        assertEquals(2, plan.size());
        assertEquals(set(concludable, concludable2), set(plan));
    }

    @Test
    public void test_planner_two_circular_relates_dependencies() {
        Concludable concludable = Concludable.create(parse("{ $a($b); }")).iterator().next();
        Concludable concludable2 = Concludable.create(parse("{ $b($a); }")).iterator().next();

        Set<Resolvable> resolvables = set(concludable, concludable2);
        List<Resolvable> plan = Resolvable.plan(resolvables);

        assertEquals(2, plan.size());
        assertEquals(set(concludable, concludable2), set(plan));
    }

    @Test
    public void test_planner_disconnected_pattern() {
        // TODO
    }
}
