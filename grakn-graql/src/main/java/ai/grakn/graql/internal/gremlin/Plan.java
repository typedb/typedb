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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static ai.grakn.graql.internal.gremlin.GraqlTraversal.fragmentCost;
import static ai.grakn.graql.internal.util.CommonUtil.toImmutableSet;

/**
 * A traversal plan for executing a Graql query, comprised of a list of fragments and a cost
 */
class Plan implements Comparable<Plan> {
    private final double cost;
    private final Fragment fragment;
    private final Plan innerPlan;
    private final Set<VarName> names;

    private Plan() {
        this.cost = 1;
        this.fragment = null;
        this.innerPlan = null;
        this.names = ImmutableSet.of();
    }

    private Plan(Fragment fragment, Plan innerPlan) {
        this.cost = fragmentCost(fragment, innerPlan.cost, innerPlan.names);
        this.fragment = fragment;
        this.innerPlan = innerPlan;
        this.names = Sets.union(innerPlan.names, fragment.getVariableNames().collect(toImmutableSet()));
    }

    static Plan base() {
        return new Plan();
    }

    Plan append(Fragment newFragment) {
        return new Plan(newFragment, this);
    }

    @Override
    public int compareTo(Plan plan) {
        return Double.compare(cost(), plan.cost());
    }

    public double cost() {
        return cost + (innerPlan != null ? innerPlan.cost() : 0);
    }

    public List<Fragment> fragments() {
        List<Fragment> fragments = new ArrayList<>();
        Plan plan = this;
        while (plan != null && plan.fragment != null) {
            assert innerPlan != null; // These are always either both null or both non-null
            fragments.add(plan.fragment);
            plan = plan.innerPlan;
        }
        return Lists.reverse(fragments);
    }

    public Set<VarName> names() {
        return names;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Plan plan = (Plan) o;

        if (Double.compare(plan.cost, cost) != 0) return false;
        if (fragment != null ? !fragment.equals(plan.fragment) : plan.fragment != null) return false;
        return innerPlan != null ? innerPlan.equals(plan.innerPlan) : plan.innerPlan == null;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(cost);
        result = (int) (temp ^ (temp >>> 32));
        result = 31 * result + (fragment != null ? fragment.hashCode() : 0);
        result = 31 * result + (innerPlan != null ? innerPlan.hashCode() : 0);
        return result;
    }
}
