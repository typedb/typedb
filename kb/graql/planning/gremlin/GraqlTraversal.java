/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.kb.graql.planning.gremlin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import grakn.core.kb.concept.api.ConceptId;
import graql.lang.statement.Variable;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Map;
import java.util.Set;

public interface GraqlTraversal {

    //            Set of disjunctions
    //                |
    //                |           List of fragments in order of execution
    //                |             |
    //                V             V
    ImmutableSet<ImmutableList<? extends Fragment>> fragments();

    /**
     * Get the {@code GraphTraversal} that this {@code GraqlTraversal} represents.
     */
    // Because 'union' accepts an array, we can't use generics
    @SuppressWarnings("unchecked")
    GraphTraversal<Vertex, Map<String, Element>> getGraphTraversal(Set<Variable> vars);

    /**
     * @param transform map defining id transform var -> new id
     * @return graql traversal with concept id transformed according to the provided transform
     */
    GraqlTraversal transform(Map<Variable, ConceptId> transform);

    double getComplexity();
}
