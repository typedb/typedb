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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Optional;

import static ai.grakn.graql.internal.util.StringConverter.idToString;
import static ai.grakn.util.Schema.EdgeLabel.SHORTCUT;
import static ai.grakn.util.Schema.EdgeProperty.FROM_ROLE_NAME;
import static ai.grakn.util.Schema.EdgeProperty.RELATION_TYPE_NAME;
import static ai.grakn.util.Schema.EdgeProperty.TO_ROLE_NAME;

class ShortcutFragment extends AbstractFragment {

    private final Optional<String> relationType;
    private final Optional<String> roleStart;
    private final Optional<String> roleEnd;

    ShortcutFragment(
            Optional<String> relationType, Optional<String> roleStart, Optional<String> roleEnd,
            String start, String end
    ) {
        super(start, end);
        this.relationType = relationType;
        this.roleStart = roleStart;
        this.roleEnd = roleEnd;
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        GraphTraversal<Vertex, Edge> edgeTraversal = traversal.outE(SHORTCUT.getLabel());
        roleStart.ifPresent(rs -> edgeTraversal.has(FROM_ROLE_NAME.name(), rs));
        roleEnd.ifPresent(re -> edgeTraversal.has(TO_ROLE_NAME.name(), re));
        relationType.ifPresent(rt -> edgeTraversal.has(RELATION_TYPE_NAME.name(), rt));
        edgeTraversal.inV();
    }

    @Override
    public String getName() {
        String start = roleStart.map(rs -> idToString(rs) + " ").orElse("");
        String type = relationType.map(rt -> ":" + idToString(rt)).orElse("");
        String end = roleEnd.map(re -> " " + idToString(re)).orElse("");
        return "-[" + start + "shortcut" + type + end + "]->";
    }

    @Override
    public double fragmentCost(double previousCost) {
        return previousCost * NUM_SHORTCUT_EDGES_PER_INSTANCE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ShortcutFragment that = (ShortcutFragment) o;

        if (relationType != null ? !relationType.equals(that.relationType) : that.relationType != null) return false;
        if (roleStart != null ? !roleStart.equals(that.roleStart) : that.roleStart != null) return false;
        return roleEnd != null ? roleEnd.equals(that.roleEnd) : that.roleEnd == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (relationType != null ? relationType.hashCode() : 0);
        result = 31 * result + (roleStart != null ? roleStart.hashCode() : 0);
        result = 31 * result + (roleEnd != null ? roleEnd.hashCode() : 0);
        return result;
    }
}
