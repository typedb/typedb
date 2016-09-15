/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.query.match;

import io.mindmaps.MindmapsGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;

import static io.mindmaps.util.Schema.ConceptProperty.*;

class MatchOrderImpl implements MatchOrder {

    private final String var;
    private final boolean asc;

    MatchOrderImpl(String var, boolean asc) {
        this.var = var;
        this.asc = asc;
    }

    @Override
    public String getVar() {
        return var;
    }

    @Override
    public void orderTraversal(MindmapsGraph graph, GraphTraversal<Vertex, Map<String, Vertex>> traversal) {
        Comparator<Optional<Comparable>> comparator = new ResourceComparator();
        if (!asc) comparator = comparator.reversed();

        // Order by VALUE properties by default
        traversal.select(var).order().by(v -> getValue(graph, v), comparator);
    }

    /**
     * Get the value of a resource
     * @param elem the element in the graph (a gremlin object)
     * @return the value of the concept, or nothing if there is no value
     */
    private Optional<Comparable> getValue(MindmapsGraph graph, Object elem) {
        return graph.getTinkerTraversal().V(elem)
                .values(VALUE_BOOLEAN.name(), VALUE_LONG.name(), VALUE_DOUBLE.name(), VALUE_STRING.name())
                .tryNext().map(o -> (Comparable) o);
    }

    @Override
    public String toString() {
        return "order by $" + var + " ";
    }

    /**
     * A comparator that parses (optionally present) resources into the correct datatype for comparison
     */
    private static class ResourceComparator implements Comparator<Optional<Comparable>> {

        @Override
        public int compare(Optional<Comparable> value1, Optional<Comparable> value2) {
            if (!value1.isPresent() && !value2.isPresent()) return 0;
            if (!value1.isPresent()) return -1;
            if (!value2.isPresent()) return 1;

            //noinspection unchecked
            return value1.get().compareTo(value2.get());
        }
    }
}
