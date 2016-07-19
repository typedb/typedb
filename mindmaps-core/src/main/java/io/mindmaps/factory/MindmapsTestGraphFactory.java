package io.mindmaps.factory;

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.implementation.MindmapsTinkerGraph;

public class MindmapsTestGraphFactory {

    public static MindmapsGraph newEmptyGraph(){
        return new MindmapsTinkerGraph();
    }
}
