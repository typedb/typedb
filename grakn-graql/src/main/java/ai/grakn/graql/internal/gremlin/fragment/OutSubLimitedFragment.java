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

package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.graql.Var;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import com.google.auto.value.AutoValue;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;

/**
 * Fragment following out sub edges
 *
 * @author Felix Chapman
 * @author Joshua Send
 */

@AutoValue
public abstract class OutSubLimitedFragment extends OutSubFragment {

    @Override
    public abstract Var end();

    @Override
    public abstract int limit();

    @Override
    public GraphTraversal<Vertex, ? extends Element> applyTraversalInner(
            GraphTraversal<Vertex, ? extends Element> traversal, EmbeddedGraknTx<?> graph, Collection<Var> vars) {
        return Fragments.outSubsLimited(Fragments.isVertex(traversal), this.limit());
    }

    @Override
    public String name() {
        return "-[sub!" + Integer.toString(this.limit()) + "]->";
    }

    @Override
    public Fragment getInverse() {
        // TODO figure out the inverse
        return Fragments.inSubLimited(varProperty(), end(), start(), this.limit());
    }

}

