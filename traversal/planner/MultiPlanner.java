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

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.traversal.procedure.GraphProcedure;
import com.vaticle.typedb.core.traversal.procedure.PermutationProcedure;
import com.vaticle.typedb.core.traversal.structure.Structure;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNEXPECTED_INTERRUPTION;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class MultiPlanner implements Planner {

    private final List<ConnectedPlanner> planners;
    private final Semaphore optimisationLock;
    private GraphProcedure procedure;

    private MultiPlanner(List<ConnectedPlanner> planners) {
        this.planners = planners;
        this.optimisationLock = new Semaphore(1);
        if (iterate(planners).allMatch(Planner::isOptimal)) createProcedure();
    }

    static MultiPlanner create(List<Structure> structures) {
        List<ConnectedPlanner> planners = new ArrayList<>(structures.size());
        structures.forEach(s -> planners.add(ConnectedPlanner.create(s)));
        return new MultiPlanner(planners);
    }

    @Override
    public void tryOptimise(GraphManager graphMgr, boolean singleUse) {
        if (optimisationLock.tryAcquire()) {
            mayOptimise(graphMgr, singleUse);
            optimisationLock.release();
        } else {
            try {
                // await current optimisation
                optimisationLock.acquire();
                optimisationLock.release();
            } catch (InterruptedException e) {
                throw TypeDBException.of(UNEXPECTED_INTERRUPTION);
            }
        }
    }

    private void mayOptimise(GraphManager graphMgr, boolean singleUse) {
        if (isOptimal()) return;
        List<CompletableFuture<Void>> futures = new ArrayList<>(planners.size());
        planners.forEach(planner -> futures.add(CompletableFuture.runAsync(() -> planner.tryOptimise(graphMgr, singleUse))));
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        createProcedure();
    }

    private void createProcedure() {
        // create procedure based on optimal ordering of all the planners
        // rather than solving the optimisation, we estimate the most expensive traversals are the largest ones
        Comparator<ConnectedPlanner> comparator = Comparator.<ConnectedPlanner, Integer>comparing(planner ->
                planner.isVertex() ? 1 : planner.asGraph().vertices().size()
        ).reversed();
        procedure = GraphProcedure.create(planners.stream().sorted(comparator).collect(Collectors.toList()));
    }

    @Override
    public PermutationProcedure procedure() {
        return procedure;
    }

    @Override
    public boolean isOptimal() {
        return iterate(planners).allMatch(Planner::isOptimal);
    }
}
