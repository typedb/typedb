/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs
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

package ai.grakn.graql.internal.query.match;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Map;

import static ai.grakn.util.Schema.ConceptProperty.VALUE_BOOLEAN;
import static ai.grakn.util.Schema.ConceptProperty.VALUE_DOUBLE;
import static ai.grakn.util.Schema.ConceptProperty.VALUE_LONG;
import static ai.grakn.util.Schema.ConceptProperty.VALUE_STRING;

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
                .values(VALUE_BOOLEAN.name(), VALUE_LONG.name(), VALUE_DOUBLE.name(), VALUE_STRING.name())
                .order().by(asc ? Order.incr : Order.decr);
    }

    @Override
    public String toString() {
        return "order by $" + var + " ";
    }
}
