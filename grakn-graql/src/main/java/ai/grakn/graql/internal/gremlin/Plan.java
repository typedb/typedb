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
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static ai.grakn.graql.internal.gremlin.GraqlTraversal.fragmentCost;

/**
 * A traversal plan for executing a Graql query, comprised of a list of fragments and a cost
 */
class Plan implements Comparable<Plan> {
    private double cost;
    private final Fragment fragment;
    private final Plan innerPlan;

    private Plan() {
        this.cost = 1;
        this.fragment = null;
        this.innerPlan = null;
    }

    private Plan(double cost, Fragment fragment, Plan innerPlan) {
        this.cost = cost + innerPlan.cost();
        this.fragment = fragment;
        this.innerPlan = innerPlan;
    }

    static Plan base() {
        return new Plan();
    }

    Plan append(Fragment newFragment, Set<VarName> names) {
        double newCost = fragmentCost(newFragment, cost, names);
        return new Plan(newCost, newFragment, this);
    }

    @Override
    public int compareTo(Plan plan) {
        return Double.compare(cost(), plan.cost());
    }

    public double cost() {
        return cost;
    }

    public List<Fragment> fragments() {
        List<Fragment> fragments = new ArrayList<>();
        Plan plan = this;
        while (plan.fragment != null) {
            assert innerPlan != null; // These are always either both null or both non-null
            fragments.add(plan.fragment);
            plan = plan.innerPlan;
        }
        return Lists.reverse(fragments);
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
