/*
 * Copyright (C) 2022 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.vaticle.typedb.core.traversal.planner;

import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.graph.TraversalVertex;
import com.vaticle.typedb.core.traversal.procedure.GraphProcedure;
import com.vaticle.typedb.core.traversal.structure.Structure;
import com.vaticle.typedb.core.traversal.structure.StructureVertex;

import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.map;
import static com.vaticle.typedb.common.collection.Collections.pair;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class VertexPlanner implements ComponentPlanner {

    private final Structure structure;
    private final GraphProcedure procedure;
    private final Set<Identifier> vertices;

    private VertexPlanner(Structure structure) {
        this.structure = structure;
        this.procedure = GraphProcedure.create(structure, map(pair(structure.vertices().iterator().next().id(), 0)));
        this.vertices = iterate(structure.vertices()).map(TraversalVertex::id).toSet();
    }

    static VertexPlanner create(Structure structure) {
        assert structure.vertices().size() == 1;
        return new VertexPlanner(structure);
    }

    @Override
    public Set<Identifier> vertices() {
        return vertices;
    }

    @Override
    public GraphProcedure procedure() {
        return procedure;
    }

    public StructureVertex<?> structureVertex() {
        return structure.vertices().iterator().next();
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
