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

import ai.grakn.GraknTx;
import ai.grakn.concept.Label;
import ai.grakn.graql.Var;
import ai.grakn.graql.internal.util.StringConverter;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Collection;
import java.util.Set;

import static ai.grakn.util.Schema.VertexProperty.LABEL_ID;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

@AutoValue
abstract class LabelFragment extends Fragment {

    abstract ImmutableSet<Label> labels();

    @Override
    public GraphTraversal<Element, ? extends Element> applyTraversalInner(
            GraphTraversal<Element, ? extends Element> traversal, GraknTx graph, Collection<Var> vars) {

        Set<Integer> labelIds =
                labels().stream().map(label -> graph.admin().convertToId(label).getValue()).collect(toSet());

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
    public double fragmentCost() {
        return COST_NODE_INDEX;
    }

    @Override
    public boolean hasFixedFragmentCost() {
        return true;
    }
}
