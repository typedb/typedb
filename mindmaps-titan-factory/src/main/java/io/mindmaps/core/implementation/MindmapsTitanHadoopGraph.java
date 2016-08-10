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

package io.mindmaps.core.implementation;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import io.mindmaps.core.MindmapsTransaction;
import org.apache.tinkerpop.gremlin.structure.Graph;

public class MindmapsTitanHadoopGraph extends MindmapsGraphImpl {
    public MindmapsTitanHadoopGraph(Graph graph, String address, String graphComputer){
        super(graph, address, graphComputer);
    }

    @Override
    public MindmapsTransaction newTransaction() {
        getGraph();
        try {
            return new MindmapsTitanHadoopTransaction(this);
        } catch (IllegalStateException e){
            throw  new GraphRuntimeException(ErrorMessage.CLOSED.getMessage(this));
        }
    }

    @Override
    public void clear() {
        TitanGraph titanGraph = ((TitanGraph) getGraph());
        titanGraph.close();
        TitanCleanup.clear(titanGraph);
    }
}
