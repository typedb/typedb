package io.mindmaps.core.implementation;

import io.mindmaps.core.model.Rule;
import io.mindmaps.core.model.RuleType;
import org.apache.tinkerpop.gremlin.structure.Vertex;

class RuleTypeImpl extends TypeImpl<RuleType, Rule> implements RuleType {
    RuleTypeImpl(Vertex v, MindmapsTransactionImpl mindmapsGraph) {
        super(v, mindmapsGraph);
    }
}
