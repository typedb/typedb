/*
 *  MindmapsDB - A Distributed Semantic Database
 *  Copyright (C) 2016  Mindmaps Research Ltd
 *
 *  MindmapsDB is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  MindmapsDB is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.factory;

import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.implementation.AbstractMindmapsGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MindmapsTitanHadoopGraphFactory extends AbstractMindmapsGraphFactory<AbstractMindmapsGraph<HadoopGraph>, HadoopGraph> {
    private final Logger LOG = LoggerFactory.getLogger(MindmapsTitanHadoopGraphFactory.class);

    @Override
    boolean isClosed(HadoopGraph innerGraph) {
        return false;
    }

    @Override
    AbstractMindmapsGraph<HadoopGraph> buildMindmapsGraphFromTinker(HadoopGraph graph, String name, String engineUrl, boolean batchLoading) {
        throw new UnsupportedOperationException(ErrorMessage.CANNOT_PRODUCE_MINDMAPS_GRAPH.getMessage(graph.getClass().getName()));
    }

    @Override
    HadoopGraph buildTinkerPopGraph(String name, String address, String pathToConfig) {
        LOG.warn("Hadoop graph ignores parameters name [" + name + "] and address [" + address + "]");
        return (HadoopGraph) GraphFactory.open(pathToConfig);
    }
}
