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

import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.VarName;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Optional;
import java.util.UUID;

import static ai.grakn.graql.internal.util.StringConverter.typeLabelToString;
import static ai.grakn.util.Schema.EdgeLabel.SHORTCUT;
import static ai.grakn.util.Schema.EdgeProperty.RELATION_TYPE_LABEL;
import static ai.grakn.util.Schema.EdgeProperty.ROLE_TYPE_LABEL;

class ShortcutFragment extends AbstractFragment {

    private final Optional<TypeLabel> relationType;
    private final Optional<TypeLabel> roleStart;
    private final Optional<TypeLabel> roleEnd;

    ShortcutFragment(
            Optional<TypeLabel> relationType, Optional<TypeLabel> roleStart, Optional<TypeLabel> roleEnd,
            VarName start, VarName end
    ) {
        super(start, end);
        this.relationType = relationType;
        this.roleStart = roleStart;
        this.roleEnd = roleEnd;
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        // TODO: Split this traversal in two
        String shortcutIn = UUID.randomUUID().toString();
        GraphTraversal<Vertex, Edge> edgeTraversal = traversal.inE(SHORTCUT.getLabel()).as(shortcutIn);
        roleStart.ifPresent(rs -> edgeTraversal.has(ROLE_TYPE_LABEL.name(), rs.getValue()));
        relationType.ifPresent(rt -> edgeTraversal.has(RELATION_TYPE_LABEL.name(), rt.getValue()));
        edgeTraversal.otherV().outE(SHORTCUT.getLabel()).where(P.neq(shortcutIn));
        roleEnd.ifPresent(re -> edgeTraversal.has(ROLE_TYPE_LABEL.name(), re.getValue()));
        edgeTraversal.inV();
    }

    @Override
    public String getName() {
        String start = roleStart.map(rs -> typeLabelToString(rs) + " ").orElse("");
        String type = relationType.map(rt -> ":" + typeLabelToString(rt)).orElse("");
        String end = roleEnd.map(re -> " " + typeLabelToString(re)).orElse("");
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

        // "simplifying" this makes it harder to read
        //noinspection SimplifiableIfStatement
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
