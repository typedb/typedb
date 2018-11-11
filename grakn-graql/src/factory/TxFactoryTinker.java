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

package grakn.core.factory;

import grakn.core.GraknTx;
import grakn.core.kb.internal.GraknTxTinker;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;


/**
 * <p>
 *     A {@link GraknTx} on top of {@link TinkerGraph}
 * </p>
 *
 * <p>
 *     This produces an in memory grakn graph on top of {@link TinkerGraph}.
 *     The base construction process defined by {@link TxFactoryAbstract} ensures the graph factories are singletons.
 * </p>
 *
 * @author fppt
 */
public class TxFactoryTinker extends TxFactoryAbstract<GraknTxTinker, TinkerGraph> {

    private TinkerGraph tinkerGraph;

    TxFactoryTinker(EmbeddedGraknSession session){
        super(session);
        tinkerGraph = TinkerGraph.open();
    }

    @Override
    protected GraknTxTinker buildGraknTxFromTinkerGraph(TinkerGraph graph) { return new GraknTxTinker(session(), graph); }

    @Override
    protected TinkerGraph buildTinkerPopGraph(boolean batchLoading) {
        return tinkerGraph;
    }

    public TinkerGraph getTinkerPopGraph(){
        return tinkerGraph;
    }

    @Override
    protected TinkerGraph getGraphWithNewTransaction(TinkerGraph graph, boolean batchLoading) {
        return tinkerGraph;
    }
}
