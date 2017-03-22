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

import java.util.List;
import java.util.Set;
import java.util.Stack;

import static ai.grakn.graql.internal.gremlin.GraqlTraversal.fragmentListCost;

/**
 * A traversal plan for executing a Graql query, comprised of a list of fragments and a cost
 */
class Plan implements Comparable<Plan> {
    private final Stack<Fragment> fragments;
    private final Set<EquivalentFragmentSet> fragmentSets;
    private double cost = -1;

    private Plan(Stack<Fragment> fragments, Set<EquivalentFragmentSet> fragmentSets) {
        this.fragments = fragments;
        this.fragmentSets = fragmentSets;
    }

    static Plan base() {
        return new Plan(new Stack<>(), Sets.newHashSet());
    }

    public Plan copy() {
        Stack<Fragment> fragmentsCopy = new Stack<>();
        fragmentsCopy.addAll(fragments);
        return new Plan(fragmentsCopy, Sets.newHashSet(fragmentSets));
    }

    boolean tryPush(Fragment newFragment) {
        if (!hasNames(newFragment.getDependencies())) {
            return false;
        }

        if (!fragmentSets.add(newFragment.getEquivalentFragmentSet())) {
            return false;
        }

        fragments.push(newFragment);

        // TODO: Calculate cost incrementally
        cost = -1;

        return true;
    }

    Fragment pop() {
        Fragment fragment = fragments.pop();
        fragmentSets.remove(fragment.getEquivalentFragmentSet());
        cost = -1;
        return fragment;
    }

    @Override
    public int compareTo(Plan plan) {
        return Double.compare(cost(), plan.cost());
    }

    public double cost() {
        if (cost == -1) {
            cost = fragmentListCost(fragments);
        }
        return cost;
    }

    public List<Fragment> fragments() {
        return fragments;
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
    public String toString() {
        return "Plan(" + GraqlTraversal.create(ImmutableSet.of(fragments)) + ")";
    }
}
