/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.gremlin.fragment;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.graql.planning.spanningtree.graph.Node;
import grakn.core.kb.graql.planning.spanningtree.graph.NodeId;
import grakn.core.kb.graql.planning.spanningtree.graph.SchemaNode;
import grakn.core.kb.server.Transaction;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import static grakn.core.core.Schema.VertexProperty.LABEL_ID;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * A fragment representing traversing a label.
 */

abstract class LabelFragment extends FragmentImpl {
    private final ImmutableSet<Label> labels;

    LabelFragment(@Nullable VarProperty varProperty, Variable start, ImmutableSet<Label> labels) {
        super(varProperty, start);
        if (labels == null) {
            throw new NullPointerException("Null labels");
        }
        this.labels = labels;
    }

    // TODO: labels() should return ONE label instead of a set
    public ImmutableSet<Label> labels() {
        return labels;
    }

    @Override
    public GraphTraversal<Vertex, ? extends Element> applyTraversalInner(
            GraphTraversal<Vertex, ? extends Element> traversal, Transaction tx, Collection<Variable> vars) {

        Set<Integer> labelIds =
                labels().stream().map(label -> tx.convertToId(label).getValue()).collect(toSet());

        if (labelIds.size() == 1) {
            int labelId = Iterables.getOnlyElement(labelIds);
            return traversal.has(LABEL_ID.name(), labelId);
        } else {
            return traversal.has(LABEL_ID.name(), P.within(labelIds));
        }
    }

    @Override
    public String name() {
        return "[label:" + labels().stream().map(Label::getValue).collect(joining(",")) + "]";
    }

    @Override
    public double internalFragmentCost() {
        return COST_NODE_INDEX;
    }

    @Override
    public boolean hasFixedFragmentCost() {
        return true;
    }

    public Long getShardCount(Transaction tx) {
        return labels().stream()
                .map(tx::<SchemaConcept>getSchemaConcept)
                .filter(schemaConcept -> schemaConcept != null && schemaConcept.isType())
                .flatMap(SchemaConcept::subs)
                .mapToLong(schemaConcept -> tx.getShardCount(schemaConcept.asType()))
                .sum();
    }

    @Override
    public Set<Node> getNodes() {
        NodeId startNodeId = NodeId.of(NodeId.Type.VAR, start());
        return Collections.singleton(new SchemaNode(startNodeId));
    }

    @Override
    public double estimatedCostAsStartingPoint(Transaction tx) {
        // there's only 1 label in this set, but sum anyway
        // estimate the total number of things that might be connected by ISA to this label as a heuristic
        long instances = labels().stream()
                .map(label -> {
                    long baseCount = tx.session().keyspaceStatistics().count(tx, label);
                    //TODO add a reasonably light estimate for inferred concepts
                    return baseCount;
                })
                .reduce(Long::sum)
                .orElseThrow(() -> new RuntimeException("LabelFragment contains no labels!"));
        return instances;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof LabelFragment) {
            LabelFragment that = (LabelFragment) o;
            return ((this.varProperty == null) ? (that.varProperty() == null) : this.varProperty.equals(that.varProperty()))
                    && (this.start.equals(that.start()))
                    && (this.labels.equals(that.labels()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= (varProperty == null) ? 0 : this.varProperty.hashCode();
        h *= 1000003;
        h ^= this.start.hashCode();
        h *= 1000003;
        h ^= this.labels.hashCode();
        return h;
    }
}
