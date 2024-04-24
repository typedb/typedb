/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.traversal.planner;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.traversal.common.Modifiers;
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
import static com.vaticle.typedb.core.concurrent.executor.Executors.async2;

public class MultiPlanner implements Planner {

    private final List<ComponentPlanner> planners;
    private final Modifiers modifiers;
    private final Semaphore optimisationLock;
    private GraphProcedure procedure;

    private MultiPlanner(List<ComponentPlanner> planners, Modifiers modifiers) {
        this.planners = planners;
        this.modifiers = modifiers;
        this.optimisationLock = new Semaphore(1);
        if (iterate(planners).allMatch(Planner::isOptimal)) createProcedure();
    }

    static MultiPlanner create(List<Structure> structures, Modifiers modifiers) {
        return new MultiPlanner(iterate(structures).map(structure -> ComponentPlanner.create(structure, modifiers)).toList(), modifiers);
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
        planners.forEach(planner -> futures.add(CompletableFuture.runAsync(() -> planner.tryOptimise(graphMgr, singleUse), async2())));
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        createProcedure();
    }

    private void createProcedure() {
        // create procedure based on optimal ordering of all the planners
        // rather than solving the optimisation, we estimate the most expensive traversals are the largest ones
        Comparator<ComponentPlanner> comparator = Comparator.<ComponentPlanner, Integer>comparing(planner -> {
            if (iterate(modifiers.sorting().variables()).anyMatch(planner.vertices()::contains)) return -1;
            else return planner.isVertex() ? 1 : planner.asGraph().vertices().size();
        }).reversed();
        List<ComponentPlanner> orderedPlanners = planners.stream().sorted(comparator).collect(Collectors.toList());
        assert orderedPlanners.get(0).vertices().containsAll(modifiers.sorting().variables());
        procedure = GraphProcedure.create(orderedPlanners);
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
