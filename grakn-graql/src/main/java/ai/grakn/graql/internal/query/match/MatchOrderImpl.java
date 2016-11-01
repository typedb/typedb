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

package ai.grakn.graql.internal.query.match;

import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Map;

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
    public void orderTraversal(GraphTraversal<Vertex, Map<String, Vertex>> traversal) {
        // Order by VALUE properties
        traversal.select(var)
                .values(Schema.ConceptProperty.VALUE_BOOLEAN.name(), Schema.ConceptProperty.VALUE_LONG.name(), Schema.ConceptProperty.VALUE_DOUBLE.name(), Schema.ConceptProperty.VALUE_STRING.name())
                .order().by(asc ? Order.incr : Order.decr);
    }

    @Override
    public String toString() {
        return "order by $" + var + " ";
    }
}
