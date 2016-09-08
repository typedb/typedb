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

import io.mindmaps.core.implementation.AbstractMindmapsGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.HashMap;
import java.util.Map;

abstract class AbstractMindmapsGraphFactory<M extends AbstractMindmapsGraph<G>, G extends Graph> implements MindmapsGraphFactory<M, G>{
    private final Map<String, M> openMindmapsGraphs;
    private final Map<String, G> openGraphs;

    AbstractMindmapsGraphFactory(){
        openMindmapsGraphs = new HashMap<>();
        openGraphs = new HashMap<>();
    }

    abstract boolean isClosed(G innerGraph);

    abstract M buildMindmapsGraphFromTinker(G graph, String name, String engineUrl, boolean batchLoading);

    abstract G buildTinkerPopGraph(String name, String address, String pathToConfig);

    @Override
    public M getGraph(String name, String address, String pathToConfig, boolean batchLoading){
        String key = generateKey(name, batchLoading);
        if(!openMindmapsGraphs.containsKey(key) || isClosed(openMindmapsGraphs.get(key))){
            openMindmapsGraphs.put(key, getMindmapsGraphFromMap(name, address, pathToConfig, batchLoading));
        }
        return openMindmapsGraphs.get(key);
    }

    @Override
    public G getTinkerPopGraph(String name, String address, String pathToConfig, boolean batchLoading){
        String key = generateKey(name, batchLoading);
        if(!openGraphs.containsKey(key) || isClosed(openGraphs.get(key))){
            openGraphs.put(key, buildTinkerPopGraph(name, address, pathToConfig));
        }
        return openGraphs.get(key);
    }

    private boolean isClosed(M mindmapsGraph) {
        G innerGraph = mindmapsGraph.getTinkerPopGraph();
        return isClosed(innerGraph);
    }

    private M getMindmapsGraphFromMap(String name, String address, String pathToConfig, boolean batchLoading) {
        String key = generateKey(name, batchLoading);

        if(!openGraphs.containsKey(key) || isClosed(openGraphs.get(key))){
            openGraphs.put(key, this.buildTinkerPopGraph(name, address, pathToConfig));
        }

        return buildMindmapsGraphFromTinker(openGraphs.get(key), name, address, batchLoading);
    }

    private String generateKey(String name, boolean batchLoading){
        return name + batchLoading;
    }
}
