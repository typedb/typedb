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
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import static ai.grakn.graql.internal.gremlin.GraqlTraversal.fragmentListCost;

/**
 * A traversal plan for executing a Graql query, comprised of a list of fragments and a cost
 */
class Plan implements Comparable<Plan> {
    private final Stack<Fragment> fragments;
    private final Multiset<VarName> names;
    private final Set<EquivalentFragmentSet> fragmentSets;

    private Plan(Stack<Fragment> fragments, Multiset<VarName> names, Set<EquivalentFragmentSet> fragmentSets) {
        this.fragments = fragments;
        this.names = names;
        this.fragmentSets = fragmentSets;
    }

    static Plan base() {
        return new Plan(new Stack<>(), HashMultiset.create(), Sets.newHashSet());
    }

    public Plan copy() {
        Stack<Fragment> fragmentsCopy = new Stack<>();
        fragmentsCopy.addAll(fragments);
        return new Plan(fragmentsCopy, HashMultiset.create(names), Sets.newHashSet(fragmentSets));
    }

    boolean tryPush(Fragment newFragment) {
        if (!names.containsAll(newFragment.getDependencies())) {
            return false;
        }

        if (!fragmentSets.add(newFragment.getEquivalentFragmentSet())) {
            return false;
        }

        fragments.push(newFragment);
        names.addAll(newFragment.getVariableNames());
        return true;
    }

    Fragment pop() {
        Fragment fragment = fragments.pop();
        names.removeAll(fragment.getVariableNames());
        fragmentSets.remove(fragment.getEquivalentFragmentSet());
        return fragment;
    }

    @Override
    public int compareTo(Plan plan) {
        return Double.compare(cost(), plan.cost());
    }

    public double cost() {
        return fragmentListCost(fragments);
    }

    public List<Fragment> fragments() {
        return fragments;
    }

    public Collection<VarName> names() {
        return names;
    }
}
