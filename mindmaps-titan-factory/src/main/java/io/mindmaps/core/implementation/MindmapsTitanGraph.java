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
import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.MindmapsTransaction;

public class MindmapsTitanGraph extends AbstractMindmapsGraph<TitanGraph> {
    public MindmapsTitanGraph(TitanGraph graph, String name, String engineUrl){
        super(graph, name, engineUrl);
    }

    @Override
    public MindmapsTransaction newTransaction() {
        try {
            return new MindmapsTitanTransaction(this);
        } catch (IllegalStateException e){
            throw  new GraphRuntimeException(ErrorMessage.CLOSED.getMessage(this));
        }
    }

    @Override
    public void clear() {
        TitanGraph titanGraph = getGraph();
        titanGraph.close();
        TitanCleanup.clear(titanGraph);

        try {
            EngineCommunicator.contactEngine(getCommitLogEndPoint(), "DELETE");
        } catch (IllegalArgumentException e){
            LOG.error(ErrorMessage.COULD_NOT_REACH_ENGINE.getMessage(getCommitLogEndPoint()), e);
        }
    }
}
