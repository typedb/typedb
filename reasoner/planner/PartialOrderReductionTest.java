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
 *
 */
package com.vaticle.typedb.core.reasoner.planner;

import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typeql.lang.TypeQL;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static junit.framework.TestCase.assertEquals;

public class PartialOrderReductionTest {

    @Test
    public void test_islands_2() {
        Resolvable<?> c1 = Concludable.create(parse("{ $a isa r1; }")).iterator().next();
        Resolvable<?> c2 = Concludable.create(parse("{ $b isa r2; }")).iterator().next();
        Set<Resolvable<?>> resolvables = new HashSet<>(set(c1, c2));

        Map<Resolvable<?>, Set<Variable>> dependencies = new HashMap<>();
        resolvables.forEach(resolvable -> dependencies.put(resolvable, set()));
        RecursivePorPlanner.PartialOrderReductionSearch porSearch = new RecursivePorPlanner.PartialOrderReductionSearch(resolvables, set(), dependencies, false);
        List<List<Resolvable<?>>> allOrderings = porSearch.allOrderings();
        assertEquals(1, allOrderings.size());
    }

    @Test
    public void test_chain_3() {
        // The relation variables need to be explicit so that they don't all wrongly become $_0
        Resolvable<?> c1 = Concludable.create(parse("{ $r0 ($a,$b) isa r1; }")).iterator().next();
        Resolvable<?> c2 = Concludable.create(parse("{ $r1 ($b,$c) isa r2; }")).iterator().next();
        Resolvable<?> c3 = Concludable.create(parse("{ $r2 ($c,$d) isa r3; }")).iterator().next();
        Set<Resolvable<?>> resolvables = set(c1, c2, c3);

        Map<Resolvable<?>, Set<Variable>> dependencies = new HashMap<>();
        resolvables.forEach(resolvable -> dependencies.put(resolvable, set()));
        RecursivePorPlanner.PartialOrderReductionSearch porSearch = new RecursivePorPlanner.PartialOrderReductionSearch(resolvables, set(), dependencies, false);
        List<List<Resolvable<?>>> allOrderings = porSearch.allOrderings();
        assertEquals(4, allOrderings.size());
    }

    @Test
    public void test_chain_4() {
        // The relation variables need to be explicit so that they don't all wrongly become $_0
        Resolvable<?> c1 = Concludable.create(parse("{ $r1 ($a,$b) isa r1; }")).iterator().next();
        Resolvable<?> c2 = Concludable.create(parse("{ $r2 ($b,$c) isa r2; }")).iterator().next();
        Resolvable<?> c3 = Concludable.create(parse("{ $r3 ($c,$d) isa r3; }")).iterator().next();
        Resolvable<?> c4 = Concludable.create(parse("{ $r4 ($d,$e) isa r4; }")).iterator().next();
        Set<Resolvable<?>> resolvables = set(c1, c2, c3, c4);

        Map<Resolvable<?>, Set<Variable>> dependencies = new HashMap<>();
        resolvables.forEach(resolvable -> dependencies.put(resolvable, set()));
        RecursivePorPlanner.PartialOrderReductionSearch porSearch = new RecursivePorPlanner.PartialOrderReductionSearch(resolvables, set(), dependencies, false);
        List<List<Resolvable<?>>> allOrderings = porSearch.allOrderings();

        assertEquals(8, allOrderings.size()); // Multiple starting points
    }

    @Test
    public void test_diamond_4() {
        // The relation variables need to be explicit so that they don't all wrongly become $_0
        Resolvable<?> c1 = Concludable.create(parse("{ $r1 ($a,$b) isa r1; }")).iterator().next();
        Resolvable<?> c2 = Concludable.create(parse("{ $r2 ($a,$c) isa r2; }")).iterator().next();
        Resolvable<?> c3 = Concludable.create(parse("{ $r3 ($b,$d) isa r3; }")).iterator().next();
        Resolvable<?> c4 = Concludable.create(parse("{ $r4 ($c,$d) isa r4; }")).iterator().next();
        Set<Resolvable<?>> resolvables = set(c1, c2, c3, c4);

        Map<Resolvable<?>, Set<Variable>> dependencies = new HashMap<>();
        resolvables.forEach(resolvable -> dependencies.put(resolvable, set()));
        RecursivePorPlanner.PartialOrderReductionSearch porSearch = new RecursivePorPlanner.PartialOrderReductionSearch(resolvables, set(), dependencies, false);
        List<List<Resolvable<?>>> allOrderings = porSearch.allOrderings();

        assertEquals(14, allOrderings.size());
    }

    @Test
    public void test_flat_tree_3() {
        // The relation variables need to be explicit so that they don't all wrongly become $_0
        Resolvable<?> c1 = Concludable.create(parse("{ $r1 ($a, $b) isa r1; }")).iterator().next();
        Resolvable<?> c2 = Concludable.create(parse("{ $a isa r2; }")).iterator().next();
        Resolvable<?> c3 = Concludable.create(parse("{ $b isa r3; }")).iterator().next();
        Set<Resolvable<?>> resolvables = set(c1, c2, c3);

        Map<Resolvable<?>, Set<Variable>> dependencies = new HashMap<>();
        resolvables.forEach(resolvable -> dependencies.put(resolvable, set()));
        RecursivePorPlanner.PartialOrderReductionSearch porSearch = new RecursivePorPlanner.PartialOrderReductionSearch(resolvables, set(), dependencies, false);
        List<List<Resolvable<?>>> allOrderings = porSearch.allOrderings();

        assertEquals(4, allOrderings.size());
    }

    @Test
    public void test_flat_tree_4() {
        // The relation variables need to be explicit so that they don't all wrongly become $_0
        Resolvable<?> c1 = Concludable.create(parse("{ $r1 ($a, $b, $c) isa r1; }")).iterator().next();
        Resolvable<?> c2 = Concludable.create(parse("{ $a isa r2; }")).iterator().next();
        Resolvable<?> c3 = Concludable.create(parse("{ $b isa r3; }")).iterator().next();
        Resolvable<?> c4 = Concludable.create(parse("{ $c isa r4; }")).iterator().next();
        Set<Resolvable<?>> resolvables = set(c1, c2, c3, c4);

        Map<Resolvable<?>, Set<Variable>> dependencies = new HashMap<>();
        resolvables.forEach(resolvable -> dependencies.put(resolvable, set()));
        RecursivePorPlanner.PartialOrderReductionSearch porSearch = new RecursivePorPlanner.PartialOrderReductionSearch(resolvables, set(), dependencies, false);
        List<List<Resolvable<?>>> allOrderings = porSearch.allOrderings();

        assertEquals(8, allOrderings.size());
    }

    @Test
    public void test_chain_4_connectedness() {
        // The relation variables need to be explicit so that they don't all wrongly become $_0
        Resolvable<?> c1 = Concludable.create(parse("{ $r1 ($a,$b) isa r1; }")).iterator().next();
        Resolvable<?> c2 = Concludable.create(parse("{ $r2 ($b,$c) isa r2; }")).iterator().next();
        Resolvable<?> c3 = Concludable.create(parse("{ $r3 ($c,$d) isa r3; }")).iterator().next();
        Resolvable<?> c4 = Concludable.create(parse("{ $r4 ($d,$e) isa r4; }")).iterator().next();
        Set<Resolvable<?>> resolvables = set(c1, c2, c3, c4);

        Map<Resolvable<?>, Set<Variable>> dependencies = new HashMap<>();
        resolvables.forEach(resolvable -> dependencies.put(resolvable, set()));
        {
            RecursivePorPlanner.PartialOrderReductionSearch porSearch = new RecursivePorPlanner.PartialOrderReductionSearch(resolvables, set(), dependencies, true);
            assertEquals(4, porSearch.allOrderings().size()); // One per start point
        }
        {
            RecursivePorPlanner.PartialOrderReductionSearch porSearch = new RecursivePorPlanner.PartialOrderReductionSearch(resolvables, getVariablesByName(resolvables, set("a")), dependencies, true);
            assertEquals(1, porSearch.allOrderings().size());
        }

        {
            RecursivePorPlanner.PartialOrderReductionSearch porSearch = new RecursivePorPlanner.PartialOrderReductionSearch(resolvables, getVariablesByName(resolvables, set("a", "e")), dependencies, true);
            assertEquals(4, porSearch.allOrderings().size());
        }
    }

    @Test
    public void test_flat_tree_4_connectedness() {
        // The relation variables need to be explicit so that they don't all wrongly become $_0
        Resolvable<?> c1 = Concludable.create(parse("{ $r1 ($a, $b, $c) isa r1; }")).iterator().next();
        Resolvable<?> c2 = Concludable.create(parse("{ $a isa r2; }")).iterator().next();
        Resolvable<?> c3 = Concludable.create(parse("{ $b isa r3; }")).iterator().next();
        Resolvable<?> c4 = Concludable.create(parse("{ $c isa r4; }")).iterator().next();
        Set<Resolvable<?>> resolvables = set(c1, c2, c3, c4);

        Map<Resolvable<?>, Set<Variable>> dependencies = new HashMap<>();
        resolvables.forEach(resolvable -> dependencies.put(resolvable, set()));
        {
            RecursivePorPlanner.PartialOrderReductionSearch porSearch = new RecursivePorPlanner.PartialOrderReductionSearch(resolvables, set(), dependencies, true);
            assertEquals(4, porSearch.allOrderings().size()); // One per start point
        }
        {
            RecursivePorPlanner.PartialOrderReductionSearch porSearch = new RecursivePorPlanner.PartialOrderReductionSearch(resolvables, getVariablesByName(resolvables, set("a")), dependencies, true);
            assertEquals(2, porSearch.allOrderings().size());
        }
    }

    @Test
    public void test_diamond_4_connectedness() {
        // The relation variables need to be explicit so that they don't all wrongly become $_0
        Resolvable<?> c1 = Concludable.create(parse("{ $r1 ($a,$b) isa r1; }")).iterator().next();
        Resolvable<?> c2 = Concludable.create(parse("{ $r2 ($a,$c) isa r2; }")).iterator().next();
        Resolvable<?> c3 = Concludable.create(parse("{ $r3 ($b,$d) isa r3; }")).iterator().next();
        Resolvable<?> c4 = Concludable.create(parse("{ $r4 ($c,$d) isa r4; }")).iterator().next();
        Set<Resolvable<?>> resolvables = set(c1, c2, c3, c4);

        Map<Resolvable<?>, Set<Variable>> dependencies = new HashMap<>();
        resolvables.forEach(resolvable -> dependencies.put(resolvable, set()));
        {
            RecursivePorPlanner.PartialOrderReductionSearch porSearch = new RecursivePorPlanner.PartialOrderReductionSearch(resolvables, set(), dependencies, true);
            assertEquals(12, porSearch.allOrderings().size()); // 3 per start point
        }
        {
            RecursivePorPlanner.PartialOrderReductionSearch porSearch = new RecursivePorPlanner.PartialOrderReductionSearch(resolvables, getVariablesByName(resolvables, set("a")), dependencies, true);
            assertEquals(6, porSearch.allOrderings().size()); // 3 per start point
        }
        {
            RecursivePorPlanner.PartialOrderReductionSearch porSearch = new RecursivePorPlanner.PartialOrderReductionSearch(resolvables, getVariablesByName(resolvables, set("a", "c")), dependencies, true);
            assertEquals(10, porSearch.allOrderings().size()); // 3 per start point
        }
    }

    private Conjunction parse(String query) {
        return Disjunction.create(TypeQL.parsePattern(query).asConjunction().normalise()).conjunctions().iterator().next();
    }

    private Set<Variable> getVariablesByName(Set<Resolvable<?>> resolvables, Set<String> names) {
        Set<Variable> vars = iterate(resolvables).flatMap(r -> iterate(r.variables()))
                .filter(v -> v.id().isName() && names.contains(v.id().asName().name()))
                .toSet();
        assert vars.size() == names.size();
        return vars;
    }

    private void debugPrint(List<Resolvable<?>> resolvables, List<String> names, List<List<Resolvable<?>>> allOrderings) {
        Map<Resolvable<?>, String> nameMap = new HashMap<>();
        for (int i = 0; i < resolvables.size(); i++) {
            nameMap.put(resolvables.get(i), names.get(i));
        }
        for (List<Resolvable<?>> ordering : allOrderings) {
            for (Resolvable<?> r : ordering) {
                System.out.print(nameMap.get(r) + " ");
            }
            System.out.println();
        }
    }
}
