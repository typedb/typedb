package com.vaticle.typedb.core.traversal.planner;

import com.vaticle.typedb.core.traversal.structure.Structure;

public interface ConnectedPlanner extends Planner {

    static ConnectedPlanner create(Structure structure) {
        assert structure.asGraphs().size() == 1;
        if (structure.vertices().size() == 1) return VertexPlanner.create(structure.vertices().iterator().next());
        else return GraphPlanner.create(structure);
    }

}
