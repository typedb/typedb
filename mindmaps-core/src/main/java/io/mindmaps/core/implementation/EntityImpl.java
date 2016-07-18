package io.mindmaps.core.implementation;

import io.mindmaps.core.model.Entity;
import io.mindmaps.core.model.EntityType;
import org.apache.tinkerpop.gremlin.structure.Vertex;

class EntityImpl extends InstanceImpl<Entity, EntityType, String> implements Entity {
    EntityImpl(Vertex v, MindmapsTransactionImpl mindmapsGraph) {
        super(v, mindmapsGraph);
    }
}