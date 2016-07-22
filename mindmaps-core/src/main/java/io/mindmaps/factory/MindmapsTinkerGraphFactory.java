package io.mindmaps.factory;

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.implementation.MindmapsTinkerGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

class MindmapsTinkerGraphFactory implements MindmapsGraphFactory {
    protected final Logger LOG = LoggerFactory.getLogger(MindmapsTinkerGraphFactory.class);
    private Map<String, MindmapsGraph> openGraphs;

    public MindmapsTinkerGraphFactory(){
        openGraphs = new HashMap<>();
    }

    @Override
    public MindmapsGraph getGraph(String name, String address, String config) {
        LOG.warn("In memory Tinkergraph ignores the address [" + address + "] and config path [" + config + "]parameters");

        if(!openGraphs.containsKey(name)){
            openGraphs.put(name, new MindmapsTinkerGraph());
        }

        return openGraphs.get(name);
    }
}
