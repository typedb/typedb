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
 */

package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.graql.VarName;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Set;

class DistinctCastingFragment extends AbstractFragment {

    private final VarName otherCastingName;

    DistinctCastingFragment(VarName start, VarName otherCastingName) {
        super(start);
        this.otherCastingName = otherCastingName;
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        traversal.where(P.neq(otherCastingName.getValue()));
    }

    @Override
    public Set<VarName> getDependencies() {
        return ImmutableSet.of(otherCastingName);
    }

    @Override
    public String getName() {
        return "[distinct-casting:" + otherCastingName.shortName() + "]";
    }

    @Override
    public double fragmentCost(double previousCost) {
        return previousCost / NUM_ROLES_PER_RELATION;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        DistinctCastingFragment that = (DistinctCastingFragment) o;

        return otherCastingName != null ? otherCastingName.equals(that.otherCastingName) : that.otherCastingName == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (otherCastingName != null ? otherCastingName.hashCode() : 0);
        return result;
    }
}
