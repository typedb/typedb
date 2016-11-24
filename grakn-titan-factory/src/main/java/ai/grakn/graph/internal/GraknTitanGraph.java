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

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;

public class GraknTitanGraph extends AbstractGraknGraph<TitanGraph> {
    public GraknTitanGraph(TitanGraph graph, String name, String engineUrl, boolean batchLoading){
        super(graph, name, engineUrl, batchLoading);
    }

    @Override
    protected void clearGraph() {
        TitanGraph titanGraph = getTinkerPopGraph();
        titanGraph.close();
        TitanCleanup.clear(titanGraph);
    }

    @Override
    public void closeGraph(String reason){
        finaliseClose(this::closeTitan, reason);
    }

    private void closeTitan(){
        StandardTitanGraph graph = (StandardTitanGraph) getTinkerPopGraph();
        synchronized (graph) { //Have to block here because the list of open transactions in Titan is not thread safe.
            graph.tx().close();
            if (graph.getOpenTransactions().isEmpty()) {
                closePermanent();
            }
        }
    }
}
