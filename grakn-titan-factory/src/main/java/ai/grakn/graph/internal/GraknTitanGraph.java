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
            System.out.println("[" + System.currentTimeMillis() + "] HERE---------> Thread [" + Thread.currentThread().getId() + "] closed transaction on [" + graph.hashCode() + "] number is now [" + graph.getOpenTransactions().size() + "]");
            if (graph.getOpenTransactions().isEmpty()) {
                //synchronized (this) { //Block on the main graph because we are about to make it unusable, so the factory should know about this.
                try {
                    Thread.sleep(2000); //SANITY CHECK FOR WAITING TO CLOSE
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                closePermanent();
                //}
            }
        }
    }
}
