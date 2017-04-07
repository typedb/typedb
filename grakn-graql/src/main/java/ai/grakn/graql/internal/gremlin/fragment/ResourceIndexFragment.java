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

package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.VarName;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static ai.grakn.util.Schema.ConceptProperty.INDEX;

class ResourceIndexFragment extends AbstractFragment {

    private final String resourceIndex;

    ResourceIndexFragment(VarName start, TypeLabel typeLabel, Object value) {
        super(start);
        this.resourceIndex = Schema.generateResourceIndex(typeLabel, value.toString());
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        traversal.has(INDEX.name(), resourceIndex);
    }

    @Override
    public String getName() {
        return "[index:" + resourceIndex + "]";
    }

    @Override
    public double fragmentCost(double previousCost) {
        return 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ResourceIndexFragment that = (ResourceIndexFragment) o;

        return resourceIndex.equals(that.resourceIndex);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + resourceIndex.hashCode();
        return result;
    }
}
