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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Set;
import java.util.Stack;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.gremlin.GraqlTraversal.fragmentCost;
import static java.util.stream.Collectors.toList;

/**
 * A traversal plan for executing a Graql query, comprised of a list of fragments and a cost
 */
class Plan implements Comparable<Plan> {
    private final Stack<PlanElement> elements;
    private final Set<EquivalentFragmentSet> fragmentSets;

    private Plan(Stack<PlanElement> elements, Set<EquivalentFragmentSet> fragmentSets) {
        this.elements = elements;
        this.fragmentSets = fragmentSets;
    }

    static Plan base() {
        return new Plan(new Stack<>(), Sets.newHashSet());
    }

    public Plan copy() {
        Stack<PlanElement> fragmentsCopy = new Stack<>();
        fragmentsCopy.addAll(elements);
        return new Plan(fragmentsCopy, Sets.newHashSet(fragmentSets));
    }

    boolean tryPush(Fragment newFragment) {
        if (!hasNames(newFragment.getDependencies())) {
            return false;
        }

        if (!fragmentSets.add(newFragment.getEquivalentFragmentSet())) {
            return false;
        }

        double cost = 1;
        double totalCost = 0;
        Set<VarName> names = ImmutableSet.of();

        if (!elements.isEmpty()) {
            PlanElement current = elements.peek();
            cost = current.cost;
            totalCost = current.totalCost;
            names = current.names;
        }

        double newCost = fragmentCost(newFragment, cost, names);
        double newTotalCost = totalCost + newCost;
        Set<VarName> newNames = Sets.union(newFragment.getVariableNames(), names);

        elements.push(new PlanElement(newFragment, newNames, newCost, newTotalCost));
        return true;
    }

    Fragment pop() {
        PlanElement element = elements.pop();
        fragmentSets.remove(element.fragment.getEquivalentFragmentSet());
        return element.fragment;
    }

    @Override
    public int compareTo(Plan plan) {
        return Double.compare(cost(), plan.cost());
    }

    public double cost() {
        return elements.peek().totalCost;
    }

    public Stream<Fragment> fragments() {
        return elements.stream().map(element -> element.fragment);
    }

    public int size() {
        return elements.size();
    }

    private boolean hasNames(Set<VarName> names) {
        if (names.isEmpty()) {
            return true;
        }

        // Create mutable copy
        names = Sets.newHashSet(names);

        for (PlanElement element : elements) {
            if (names.removeAll(element.fragment.getVariableNames()) && names.isEmpty()) {
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

        if (!elements.equals(plan.elements)) return false;
        return fragmentSets.equals(plan.fragmentSets);
    }

    @Override
    public int hashCode() {
        int result = elements.hashCode();
        result = 31 * result + fragmentSets.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Plan(" + GraqlTraversal.create(ImmutableSet.of(fragments().collect(toList()))) + ")";
    }

    private static class PlanElement {
        private final Fragment fragment;
        private final Set<VarName> names;
        private final double cost;
        private final double totalCost;

        private PlanElement(Fragment fragment, Set<VarName> names, double cost, double totalCost) {
            this.fragment = fragment;
            this.names = names;
            this.cost = cost;
            this.totalCost = totalCost;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PlanElement that = (PlanElement) o;

            if (Double.compare(that.cost, cost) != 0) return false;
            if (Double.compare(that.totalCost, totalCost) != 0) return false;
            if (!fragment.equals(that.fragment)) return false;
            return names.equals(that.names);
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            result = fragment.hashCode();
            result = 31 * result + names.hashCode();
            temp = Double.doubleToLongBits(cost);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(totalCost);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            return result;
        }
    }
}
