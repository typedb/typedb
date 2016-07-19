package io.mindmaps.factory;

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.implementation.MindmapsTinkerGraph;

public class MindmapsTinkerGraphFactory implements MindmapsGraphFactory {
    private static MindmapsTinkerGraphFactory factoryInstance;

    public MindmapsTinkerGraphFactory(){

    }

    public static MindmapsGraphFactory getInstance(){
        if(factoryInstance == null){
            factoryInstance = new MindmapsTinkerGraphFactory();
        }
        return factoryInstance;
    }

    @Override
    public MindmapsGraph newGraph(String... config) {
        return new MindmapsTinkerGraph();
    }
}
