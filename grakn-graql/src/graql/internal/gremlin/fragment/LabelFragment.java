/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.graql.internal.gremlin.fragment;

import grakn.core.concept.Label;
import grakn.core.concept.SchemaConcept;
import grakn.core.graql.Var;
import grakn.core.graql.internal.util.StringConverter;
import grakn.core.kb.internal.EmbeddedGraknTx;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.Set;

import static grakn.core.util.Schema.VertexProperty.LABEL_ID;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * A fragment representing traversing a label.
 *
 */

@AutoValue
public abstract class LabelFragment extends Fragment {

    // TODO: labels() should return ONE label instead of a set
    public abstract ImmutableSet<Label> labels();

    @Override
    public GraphTraversal<Vertex, ? extends Element> applyTraversalInner(
            GraphTraversal<Vertex, ? extends Element> traversal, EmbeddedGraknTx<?> tx, Collection<Var> vars) {

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
        return "[label:" + labels().stream().map(StringConverter::typeLabelToString).collect(joining(",")) + "]";
    }

    @Override
    public double internalFragmentCost() {
        return COST_NODE_INDEX;
    }

    @Override
    public boolean hasFixedFragmentCost() {
        return true;
    }

    @Override
    public Long getShardCount(EmbeddedGraknTx<?> tx) {
        return labels().stream()
                .map(tx::<SchemaConcept>getSchemaConcept)
                .filter(schemaConcept -> schemaConcept != null && schemaConcept.isType())
                .flatMap(SchemaConcept::subs)
                .mapToLong(schemaConcept -> tx.getShardCount(schemaConcept.asType()))
                .sum();
    }
}
