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

package ai.grakn.graph.internal;

import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class GraknOrientDBGraph extends AbstractGraknGraph<OrientGraph> {
    public GraknOrientDBGraph(OrientGraph graph, String name, String engineUrl, boolean batchLoading){
        super(graph, name, engineUrl, batchLoading);
    }

    @Override
    protected void commitTx(){
        getTinkerPopGraph().commit();
    }

    @Override
    public GraphTraversal<Vertex, Vertex> getTinkerTraversal(){
        Schema.BaseType[] baseTypes = Schema.BaseType.values();
        String [] labels = new String [baseTypes.length];

        for(int i = 0; i < labels.length; i ++){
            labels[i] = baseTypes[i].name();
        }

        return getTinkerPopGraph().traversal().withStrategies(ReadOnlyStrategy.instance()).V().hasLabel(labels);
    }

    @Override
    public void rollback(){
        throw new UnsupportedOperationException(ErrorMessage.UNSUPPORTED_GRAPH.getMessage(getTinkerPopGraph().getClass().getName(), "rollback"));
    }
}
