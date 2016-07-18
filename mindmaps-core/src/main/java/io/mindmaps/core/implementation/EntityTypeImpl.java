package io.mindmaps.core.implementation;

import io.mindmaps.core.model.Entity;
import io.mindmaps.core.model.EntityType;
import org.apache.tinkerpop.gremlin.structure.Vertex;

class EntityTypeImpl extends TypeImpl<EntityType, Entity> implements EntityType{
    EntityTypeImpl(Vertex v, MindmapsTransactionImpl mindmapsGraph) {
        super(v, mindmapsGraph);
    }
}
