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

package com.vaticle.typedb.core.reasoner.planner;

import com.vaticle.typedb.common.collection.Collections;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.reasoner.processor.reactive.PoolingStream;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class ConjunctionStreamPlan {

    private final Set<Retrievable> identifierVariables; // If the identifier variables match, the results will match
    private final Set<Retrievable> extendOutputWith; // The variables in mergeWithRemainingVars
    private final Set<Retrievable> outputVariables;  // Strip out everything other than these.

    public ConjunctionStreamPlan(Set<Retrievable> identifierVariables, Set<Retrievable> extendOutputWith, Set<Retrievable> outputVariables) {
        this.identifierVariables = identifierVariables;
        this.extendOutputWith = extendOutputWith;
        this.outputVariables = outputVariables;
    }

    public static ConjunctionStreamPlan createConjunctionStreamPlan(List<Resolvable<?>> resolvableOrder, Set<Retrievable> inputVariables, Set<Retrievable> outputVariables) {
        Set<Retrievable> conjunctionVariables = iterate(resolvableOrder).flatMap(resolvable -> iterate(resolvable.retrieves())).toSet();
        assert iterate(inputVariables).allMatch(v -> conjunctionVariables.contains(v) || outputVariables.contains(v));
        assert iterate(outputVariables).allMatch(v -> conjunctionVariables.contains(v) || inputVariables.contains(v));
        Set<Retrievable> extension = intersection(inputVariables, outputVariables);
        Set<Retrievable> identifiers = intersection(inputVariables, conjunctionVariables);

        if (resolvableOrder.size() == 1) {
            return new ResolvablePlan(resolvableOrder.get(0), identifiers, extension, difference(outputVariables, extension));
        }

        // Chunk them into the children
        List<List<Resolvable<?>>> chunks = new ArrayList<>();
        {
            Set<Retrievable> previouslyBoundVariables = new HashSet<>(inputVariables);
            Queue<Resolvable<?>> remaining = new LinkedList<>(resolvableOrder);
            Resolvable<?> firstResolvable = remaining.poll();
            List<Resolvable<?>> currentChunk = list(firstResolvable);
            Set<Retrievable> currentChunkVariables = new HashSet<>(firstResolvable.retrieves());

            while (!remaining.isEmpty()) {
                Resolvable<?> resolvable = remaining.poll();
                if (intersection(difference(resolvable.retrieves(), currentChunkVariables), previouslyBoundVariables).size() > 0) {
                    assert !currentChunk.isEmpty();
                    chunks.add(currentChunk);
                    previouslyBoundVariables.addAll(currentChunkVariables);
                    currentChunk = new ArrayList<>();
                    currentChunkVariables = new HashSet<>();
                }
                currentChunk.add(resolvable);
                currentChunkVariables.addAll(resolvable.retrieves());
                if (iterate(resolvable.retrieves()).anyMatch(v -> outputVariables.contains(v) && !inputVariables.contains(v))) {
                    // If we find a bound, the entire suffix goes to a new level.
                    chunks.add(currentChunk);
                    currentChunkVariables = new HashSet<>();
                    currentChunk = new ArrayList<>(remaining);
                    remaining.clear();
                }
            }

            assert !currentChunk.isEmpty();
            chunks.add(currentChunk);
            if (!remaining.isEmpty()) {
                chunks.add(new ArrayList<>(remaining));
            }
        }

        // Plan each child
        List<ConjunctionStreamPlan> subPlans = new ArrayList<>();
        {
            Queue<List<Resolvable<?>>> remaining = new LinkedList<>(chunks);
            Set<Retrievable> currentlyBound = new HashSet<>(inputVariables);

            while (!remaining.isEmpty()) {
                List<Resolvable<?>> chunk = remaining.poll();
                Set<Retrievable> chunkInputs = new HashSet<>();

                ConjunctionStreamPlan subPlan = ConjunctionStreamPlan.createConjunctionStreamPlan(chunk, chunkInputs, chunkOutputs);
                subPlans.add(subPlan);
                currentlyBound.add(subPlan.outputVariables());
            }
        }

        return new CompoundStreamPlan(subPlans, inputVariables, extension, outputVariables);
    }

    private static ConjunctionStreamPlan createPlanForChunk(List<Resolvable<?>> currentChunk, Set<Retrievable> previouslyBoundVariables, Set<Retrievable> chunkVariables,
                                                            Set<Retrievable> parentInputVariables, Set<Retrievable> parentOutputVariables) {

        Set<Retrievable> chunkInputVariables = new HashSet<>();
        Set<Retrievable> chunkOutputVariables = new HashSet<>();



        return createConjunctionStreamPlan(currentChunk, chunkInputVariables, chunkOutputVariables);
    }

    public static ConjunctionStreamPlan createConjunctionStreamPlanOld(List<Resolvable<?>> resolvableOrder, Set<Retrievable> inputVariables, Set<Retrievable> outputVariables) {
        Set<Retrievable> conjunctionVariables = iterate(resolvableOrder).flatMap(resolvable -> iterate(resolvable.retrieves())).toSet();
        assert iterate(inputVariables).allMatch(v -> conjunctionVariables.contains(v) || outputVariables.contains(v));
        assert iterate(outputVariables).allMatch(v -> conjunctionVariables.contains(v) || inputVariables.contains(v));
        Set<Retrievable> extension = intersection(inputVariables, outputVariables);
        Set<Retrievable> identifiers = intersection(inputVariables, conjunctionVariables);

        // TODO: Optimise. (This just replicates the existing system)
        List<Resolvable<?>> suffix = new ArrayList<>(resolvableOrder);
        suffix.remove(0);
        Set<Retrievable> suffixVariables = iterate(suffix).flatMap(resolvable -> iterate(resolvable.retrieves())).toSet();

        Resolvable<?> leadingResolvable = resolvableOrder.get(0);
        Set<Retrievable> resolvableIdentifiers = intersection(identifiers, leadingResolvable.retrieves());
        Set<Retrievable> resolvableOutputs = intersection(leadingResolvable.retrieves(), union(outputVariables, suffixVariables));

        Set<Retrievable> boundsPostResolvable = union(identifiers, resolvableOutputs);

        Set<Retrievable> suffixIdentifiers = intersection(boundsPostResolvable, suffixVariables); // includes the extension ones
        Set<Retrievable> freshBounds = difference(boundsPostResolvable, identifiers);
        Set<Retrievable> suffixInputs = union(freshBounds, suffixIdentifiers);
        Set<Retrievable> suffixOutputs = union(intersection(outputVariables, suffixVariables), freshBounds);

        if (suffix.isEmpty()) {
            assert inputVariables.equals(union(resolvableIdentifiers, extension)) && outputVariables.equals(union(extension, resolvableOutputs));
            ResolvablePlan resolvablePlan = new ResolvablePlan(leadingResolvable, resolvableIdentifiers, extension, resolvableOutputs);
            return resolvablePlan;
        } else {
            ResolvablePlan resolvablePlan = new ResolvablePlan(leadingResolvable, resolvableIdentifiers, set(), resolvableOutputs);
            ConjunctionStreamPlan suffixPlan = createConjunctionStreamPlan(suffix, suffixInputs, suffixOutputs);
            return new CompoundStreamPlan(list(resolvablePlan, suffixPlan), inputVariables, difference(outputVariables, suffixOutputs), outputVariables);
        }
    }

    private static Set<Retrievable> union(Set<Retrievable> a, Set<Retrievable> b) {
        Set<Retrievable> result = new HashSet<>(a);
        result.addAll(b);
        return result;
    }

    private static Set<Retrievable> intersection(Set<Retrievable> a, Set<Retrievable> b) {
        Set<Retrievable> result = new HashSet<>(a);
        result.retainAll(b);
        return result;
    }

    private static Set<Retrievable> difference(Set<Retrievable> a, Set<Retrievable> b) {
        Set<Retrievable> result = new HashSet<>(a);
        result.removeAll(b);
        return result;
    }

    public boolean isResolvable() {
        return false;
    }

    public boolean isCompoundStream() {
        return false;
    }

    public ResolvablePlan asResolvablePlan() {
        throw TypeDBException.of(ILLEGAL_CAST, this.getClass(), className(ResolvablePlan.class));
    }

    public CompoundStreamPlan asCompoundStreamPlan() {
        throw TypeDBException.of(ILLEGAL_CAST, this.getClass(), className(CompoundStreamPlan.class));
    }

    public Set<Retrievable> outputVariables() {
        return outputVariables;
    }

    public Set<Retrievable> identifierVariables() {
        return identifierVariables;
    }

    public Set<Retrievable> extendOutputWithVariables() {
        return extendOutputWith;
    }

    public static class ResolvablePlan extends ConjunctionStreamPlan {
        private final Resolvable<?> resolvable;

        public ResolvablePlan(Resolvable<?> resolvable, Set<Retrievable> identifierVariables, Set<Retrievable> extendOutputWith, Set<Retrievable> outputVariables) {
            super(identifierVariables, extendOutputWith, outputVariables);
            this.resolvable = resolvable;
        }

        @Override
        public boolean isResolvable() {
            return true;
        }

        @Override
        public ResolvablePlan asResolvablePlan() {
            return this;
        }

        public Resolvable<?> resolvable() {
            return resolvable;
        }
    }

    public static class CompoundStreamPlan extends ConjunctionStreamPlan {
        private final List<ConjunctionStreamPlan> subPlans;
        private final ConcurrentHashMap<ConceptMap, PoolingStream.BufferedFanStream<ConceptMap>> compoundStreamRegistry;

        public CompoundStreamPlan(List<ConjunctionStreamPlan> subPlans, Set<Retrievable> identifierVariables, Set<Retrievable> extendOutputWith, Set<Retrievable> outputVariables) {
            super(identifierVariables, extendOutputWith, outputVariables);
            this.subPlans = subPlans;
            compoundStreamRegistry = new ConcurrentHashMap<>();
        }

        @Override
        public boolean isCompoundStream() {
            return true;
        }

        @Override
        public CompoundStreamPlan asCompoundStreamPlan() {
            return this;
        }

        public ConjunctionStreamPlan ithBranch(int i) {
            return subPlans.get(i);
        }

        public int size() {
            return subPlans.size();
        }

        public ConcurrentHashMap<ConceptMap, PoolingStream.BufferedFanStream<ConceptMap>> compoundStreamRegistry() {
            return compoundStreamRegistry;
        }
    }
}
