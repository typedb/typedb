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

import io.mindmaps.core.MindmapsGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.function.TriFunction;

import java.util.HashMap;
import java.util.Map;

abstract class MindmapsGraphFactoryImpl<M extends MindmapsGraph, T extends Graph> implements MindmapsGraphFactory<M, T>{
    final Map<String, M> openMindmapsGraphs;
    final Map<String, T> openGraphs;

    MindmapsGraphFactoryImpl(){
        openMindmapsGraphs = new HashMap<>();
        openGraphs = new HashMap<>();
    }

    abstract boolean isClosed(T innerGraph);

    abstract M buildMindmapsGraphFromTinker(T graph, String engineUrl);

    abstract T buildTinkerPopGraph(String name, String address, String pathToConfig);

    @Override
    public M getGraph(String name, String address, String pathToConfig){

        String key = name + address;
        if(!openMindmapsGraphs.containsKey(key) || isClosed(openMindmapsGraphs.get(key))){
            openMindmapsGraphs.put(key, getMindmapsGraphFromMap(name, address, pathToConfig));
        }
        return openMindmapsGraphs.get(key);
    }

    @Override
    public T getTinkerPopGraph(String name, String address, String pathToConfig){
        String key = name + address;
        if(!openGraphs.containsKey(key) || isClosed(openGraphs.get(key))){
            openGraphs.put(key, buildTinkerPopGraph(name, address, pathToConfig));
        }
        return openGraphs.get(key);
    }

    private boolean isClosed(M mindmapsGraph) {
        //TODO: Maybe add this generic to mindmaps graph
        T innerGraph = (T) mindmapsGraph.getGraph();
        return isClosed(innerGraph);
    }

    private M getMindmapsGraphFromMap(String name, String address, String pathToConfig) {
        String key = name + address;

        if(!openGraphs.containsKey(key) || isClosed(openGraphs.get(key))){
            openGraphs.put(key, this.buildTinkerPopGraph(name, address, pathToConfig));
        }

        return buildMindmapsGraphFromTinker(openGraphs.get(key), address);
    }
}
