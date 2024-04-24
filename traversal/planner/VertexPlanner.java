/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.traversal.planner;

import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.procedure.VertexProcedure;
import com.vaticle.typedb.core.traversal.structure.StructureVertex;

import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;

public class VertexPlanner implements ComponentPlanner {

    private final StructureVertex<?> structureVertex;
    private final VertexProcedure procedure;

    private VertexPlanner(StructureVertex<?> structureVertex) {
        this.structureVertex = structureVertex;
        this.procedure = VertexProcedure.create(structureVertex);
    }

    static VertexPlanner create(StructureVertex<?> structureVertex) {
        return new VertexPlanner(structureVertex);
    }

    @Override
    public Set<Identifier> vertices() {
        return set(structureVertex.id());
    }

    @Override
    public VertexProcedure procedure() {
        return procedure;
    }

    public StructureVertex<?> structureVertex() {
        return structureVertex;
    }

    @Override
    public void tryOptimise(GraphManager graphMgr, boolean singleUse) {
        assert this.procedure != null;
    }

    @Override
    public boolean isVertex() {
        return true;
    }

    @Override
    public VertexPlanner asVertex() {
        return this;
    }

    @Override
    public boolean isOptimal() {
        return true;
    }
}
