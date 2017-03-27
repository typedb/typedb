/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 */

package ai.grakn.graql.internal.gremlin;

import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;
import java.util.Stack;

import static ai.grakn.graql.internal.gremlin.GraqlTraversal.fragmentCost;

/**
 * A traversal plan for executing a Graql query, comprised of a list of fragments and a cost
 */
class Plan implements Comparable<Plan> {
    private final Stack<Fragment> fragments;
    private final Stack<Double> costs;
    private final Set<EquivalentFragmentSet> fragmentSets;
    private final Multiset<VarName> names;
    private double totalCost;

    private Plan(Stack<Fragment> fragments, Stack<Double> costs, Set<EquivalentFragmentSet> fragmentSets, Multiset<VarName> names, double totalCost) {
        this.fragments = fragments;
        this.costs = costs;
        this.fragmentSets = fragmentSets;
        this.names = names;
        this.totalCost = totalCost;
    }

    static Plan base() {
        return new Plan(new Stack<>(), new Stack<>(), Sets.newHashSet(), HashMultiset.create(), 0);
    }

    public Plan copy() {
        Stack<Fragment> fragmentsCopy = new Stack<>();
        fragmentsCopy.addAll(fragments);
        Stack<Double> costsCopy = new Stack<>();
        costsCopy.addAll(costs);
        return new Plan(fragmentsCopy, costsCopy, Sets.newHashSet(fragmentSets), HashMultiset.create(names), totalCost);
    }

    boolean tryPush(Fragment newFragment) {
        if (!hasNames(newFragment.getDependencies())) {
            return false;
        }

        if (!fragmentSets.add(newFragment.getEquivalentFragmentSet())) {
            return false;
        }

        double cost = !costs.isEmpty() ? costs.peek() : 1;

        double newCost = fragmentCost(newFragment, cost, names);
        totalCost += newCost;

        names.addAll(newFragment.getVariableNames());

        fragments.push(newFragment);
        costs.push(newCost);
        return true;
    }

    Fragment pop() {
        Fragment fragment = fragments.pop();
        fragmentSets.remove(fragment.getEquivalentFragmentSet());
        Multisets.removeOccurrences(names, fragment.getVariableNames());
        totalCost -= costs.pop();
        return fragment;
    }

    @Override
    public int compareTo(Plan plan) {
        return Double.compare(cost(), plan.cost());
    }

    public double cost() {
        return totalCost;
    }

    public List<Fragment> fragments() {
        return fragments;
    }

    public int size() {
        return fragments.size();
    }

    private boolean hasNames(Set<VarName> names) {
        if (names.isEmpty()) {
            return true;
        }

        // Create mutable copy
        names = Sets.newHashSet(names);

        for (Fragment fragment : fragments) {
            if (names.removeAll(fragment.getVariableNames()) && names.isEmpty()) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Plan plan = (Plan) o;

        if (!fragments.equals(plan.fragments)) return false;
        return fragmentSets.equals(plan.fragmentSets);
    }

    @Override
    public int hashCode() {
        int result = fragments.hashCode();
        result = 31 * result + fragmentSets.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Plan(" + GraqlTraversal.create(ImmutableSet.of(fragments())) + ", " + totalCost + ")";
    }
}
