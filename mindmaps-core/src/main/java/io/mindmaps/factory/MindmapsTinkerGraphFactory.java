package io.mindmaps.factory;

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.implementation.MindmapsTinkerGraph;

class MindmapsTinkerGraphFactory implements MindmapsGraphFactory {
    public MindmapsTinkerGraphFactory(){

    }

    @Override
    public MindmapsGraph newGraph(String... config) {
        return new MindmapsTinkerGraph();
    }
}
