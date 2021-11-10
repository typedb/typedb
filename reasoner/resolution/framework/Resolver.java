/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.reasoner.resolution.framework;

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.poller.Poller;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.producer.Producer;
import com.vaticle.typedb.core.concurrent.producer.Producers;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial;
import com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer.Trace;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response.Answer;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response.Blocked.Cycle;
import com.vaticle.typedb.core.traversal.GraphTraversal;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.parameters.Arguments.Query.Producer.INCREMENTAL;
import static com.vaticle.typedb.core.common.poller.Pollers.poll;

public abstract class Resolver<RESOLVER extends ReasonerActor<RESOLVER>> extends ReasonerActor<RESOLVER> {

    private static final Logger LOG = LoggerFactory.getLogger(Resolver.class);

    private final Map<Request.Visit, Request.Visit> requestRouter;
    protected final ResolverRegistry registry;

    protected Resolver(Driver<RESOLVER> driver, String name, ResolverRegistry registry) {
        super(driver, name);
        this.registry = registry;
        this.requestRouter = new HashMap<>();
        // Note: initialising downstream actors in constructor will create all actors ahead of time, so it is non-lazy
        // additionally, it can cause deadlock within ResolverRegistry as different threads initialise actors
    }

    @Override
    protected void exception(Throwable e) {
        if (e instanceof TypeDBException && ((TypeDBException) e).code().isPresent()) {
            String code = ((TypeDBException) e).code().get();
            if (code.equals(RESOURCE_CLOSED.code())) {
                LOG.debug("Resolver interrupted by resource close: {}", e.getMessage());
                registry.terminate(e);
                return;
            }
        }
        LOG.error("Actor exception", e);
        registry.terminate(e);
    }

    public abstract void receiveVisit(Request.Visit fromUpstream);

    protected abstract void receiveRevisit(Request.Revisit fromUpstream);

    protected abstract void receiveAnswer(Answer fromDownstream);

    protected abstract void receiveFail(Response.Fail fromDownstream);

    protected abstract void receiveBlocked(Response.Blocked fromDownstream);

    protected abstract void initialiseDownstreamResolvers(); //TODO: This method should only be required of the coordinating actors

    protected Request.Visit fromUpstream(Request.Visit toDownstream) {
        assert requestRouter.containsKey(toDownstream);
        assert requestRouter.get(toDownstream).trace() == toDownstream.trace();
        return requestRouter.get(toDownstream);
    }

    protected Request upstreamRequest(Response response) {
        return fromUpstream(response.sourceRequest().visit());
    }

    protected Partial<?> partialFromUpstream(Response response) {
        return upstreamRequest(response).visit().partialAnswer();
    }

    protected void visitDownstream(Request.Factory downstream, Request fromUpstream) {
        visitDownstream(downstream.createVisit(fromUpstream.trace()), fromUpstream);
    }

    protected void visitDownstream(Request.Visit visit, Request fromUpstream) {
        LOG.trace("{} : Sending a new Visit request to downstream: {}", name(), visit);
        if (registry.resolutionTracing()) ResolutionTracer.get().visit(visit);
        requestRouter.put(visit, fromUpstream.visit());
        visit.receiver().execute(actor -> actor.receiveVisit(visit));
    }

    protected void revisitDownstream(Request.Revisit revisit, Request fromUpstream) {
        LOG.trace("{} : Sending a new Revisit request to downstream: {}", name(), revisit);
        if (registry.resolutionTracing()) ResolutionTracer.get().revisit(revisit);
        requestRouter.put(revisit.visit(), fromUpstream.visit());
        revisit.visit().receiver().execute(actor -> actor.receiveRevisit(revisit));
    }

    protected void answerToUpstream(AnswerState answer, Request fromUpstream) {
        assert answer.isPartial();
        Answer response = Answer.create(fromUpstream.visit(), answer.asPartial(), fromUpstream.trace());
        LOG.trace("{} : Sending a new Response.Answer to upstream", name());
        if (registry.resolutionTracing()) ResolutionTracer.get().responseAnswer(response);
        fromUpstream.visit().sender().execute(actor -> actor.receiveAnswer(response));
    }

    protected void failToUpstream(Request fromUpstream) {
        Response.Fail response = new Response.Fail(fromUpstream.visit(), fromUpstream.trace());
        LOG.trace("{} : Sending a new Response.Answer to upstream", name());
        if (registry.resolutionTracing()) ResolutionTracer.get().responseExhausted(response);
        fromUpstream.visit().sender().execute(actor -> actor.receiveFail(response));
    }

    protected void blockToUpstream(Request fromUpstream, Set<Cycle> cycles) {
        assert !fromUpstream.visit().partialAnswer().parent().isTop();
        Response.Blocked response = new Response.Blocked(fromUpstream.visit(), cycles, fromUpstream.trace());
        LOG.trace("{} : Sending a new Response.Blocked to upstream", name());
        if (registry.resolutionTracing()) ResolutionTracer.get().responseBlocked(response);
        fromUpstream.visit().sender().execute(actor -> actor.receiveBlocked(response));
    }

    protected FunctionalIterator<ConceptMap> traversalIterator(Conjunction conjunction, ConceptMap bounds) {
        return compatibleBounds(conjunction, bounds).map(c -> {
            GraphTraversal.Thing traversal = boundTraversal(conjunction.traversal(), c);
            return registry.traversalEngine().iterator(traversal).map(v -> registry.conceptManager().conceptMap(v));
        }).orElse(Iterators.empty());
    }

    protected Producer<ConceptMap> traversalProducer(Conjunction conjunction, ConceptMap bounds, int parallelisation) {
        return compatibleBounds(conjunction, bounds).map(b -> {
            GraphTraversal.Thing traversal = boundTraversal(conjunction.traversal(), b);
            return registry.traversalEngine().producer(traversal, Either.first(INCREMENTAL), parallelisation)
                    .map(vertexMap -> registry.conceptManager().conceptMap(vertexMap));
        }).orElse(Producers.empty());
    }

    private Optional<ConceptMap> compatibleBounds(Conjunction conjunction, ConceptMap bounds) {
        Map<Retrievable, Concept> newBounds = new HashMap<>();
        for (Map.Entry<Retrievable, ? extends Concept> entry : bounds.concepts().entrySet()) {
            Retrievable id = entry.getKey();
            Concept bound = entry.getValue();
            Variable conjVariable = conjunction.variable(id);
            assert conjVariable != null;
            if (conjVariable.isThing()) {
                if (!conjVariable.asThing().iid().isPresent()) newBounds.put(id, bound);
                else if (!conjVariable.asThing().iid().get().iid().equals(bound.asThing().getIID())) {
                    return Optional.empty();
                }
            } else if (conjVariable.isType()) {
                if (!conjVariable.asType().label().isPresent()) newBounds.put(id, bound);
                else if (!conjVariable.asType().label().get().properLabel().equals(bound.asType().getLabel())) {
                    return Optional.empty();
                }
            } else {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
        }
        return Optional.of(new ConceptMap(newBounds));
    }

    protected static GraphTraversal.Thing boundTraversal(GraphTraversal.Thing traversal, ConceptMap bounds) {
        bounds.concepts().forEach((id, concept) -> {
            if (concept.isThing()) traversal.iid(id.asVariable(), concept.asThing().getIID());
            else {
                traversal.clearLabels(id.asVariable());
                traversal.labels(id.asVariable(), concept.asType().getLabel());
            }
        });
        return traversal;
    }

    public abstract static class ResolutionState {

        protected final Partial<?> fromUpstream;

        protected ResolutionState(Partial<?> fromUpstream) {
            this.fromUpstream = fromUpstream;
        }

        public Partial<?> fromUpstream() {
            return fromUpstream;
        }

        public abstract Optional<? extends Partial<?>> nextAnswer();

        public interface Exploration {

            boolean newAnswer(Partial<?> partial);

            ExplorationManager explorationManager();

            boolean singleAnswerRequired();

        }

    }

    public abstract static class CachingResolutionState<ANSWER> extends ResolutionState {

        protected final AnswerCache<ANSWER> answerCache;
        protected Poller<? extends Partial<?>> cacheReader;
        protected final Set<ConceptMap> deduplicationSet;

        protected CachingResolutionState(Partial<?> fromUpstream, AnswerCache<ANSWER> answerCache, boolean deduplicate) {
            super(fromUpstream);
            this.answerCache = answerCache;
            this.deduplicationSet = deduplicate ? new HashSet<>() : null;
            this.cacheReader = answerCache.reader().flatMap(
                    a -> poll(toUpstream(a).filter(
                            partial -> !deduplicate || !deduplicationSet.contains(partial.conceptMap()))));
        }

        @Override
        public Optional<? extends Partial<?>> nextAnswer() {
            Optional<? extends Partial<?>> ans = cacheReader.poll();
            if (ans.isPresent() && deduplicationSet != null) deduplicationSet.add(ans.get().conceptMap());
            return ans;
        }

        protected abstract FunctionalIterator<? extends Partial<?>> toUpstream(ANSWER answer);

    }

    public static class ExplorationManager {
        protected final List<Request.Factory> visits;
        protected final Map<Request.Factory, Set<Cycle>> revisits;
        protected final Map<Request.Factory, Set<Cycle>> blocked;

        public ExplorationManager() {
            this.visits = new ArrayList<>();
            this.revisits = new LinkedHashMap<>();
            this.blocked = new LinkedHashMap<>();
        }

        public ExplorationManager(List<Request.Factory> visits) {
            this.visits = visits;
            this.revisits = new LinkedHashMap<>();
            this.blocked = new LinkedHashMap<>();
        }

        public Request.Visit nextVisit(Trace trace) {
            return visits.get(0).createVisit(trace);
        }

        public Request.Revisit nextRevisit(Trace trace) {
            Optional<Request.Factory> downstream = iterate(revisits.keySet()).first();
            assert downstream.isPresent();
            assert !revisits.get(downstream.get()).isEmpty();
            return downstream.get().createRevisit(trace, set(revisits.get(downstream.get())));
        }

        public boolean hasNextVisit() {
            return !visits.isEmpty();
        }

        public boolean hasNextRevisit() {
            return !revisits.isEmpty();
        }

        public boolean hasNextBlocked() {
            return !blocked.isEmpty();
        }

        public boolean contains(Request.Factory downstream) {
            return visits.contains(downstream) || revisits.containsKey(downstream) || blocked.containsKey(downstream);
        }

        public void add(Request.Factory downstream) {
            assert !(visits.contains(downstream)) : "downstream answer producer already contains this request";
            visits.add(downstream);
        }

        public void remove(Request.Factory downstream) {
            visits.remove(downstream);
            revisits.remove(downstream);
            blocked.remove(downstream);
        }

        public void clear() {
            visits.clear();
            revisits.clear();
            blocked.clear();
        }

        public Set<Cycle> blockingCycles() {
            return iterate(blocked.values()).flatMap(Iterators::iterate).toSet();
        }

        public void block(Request.Factory toBlock, Set<Cycle> cycles) {
            assert contains(toBlock) && !cycles.isEmpty();
            visits.remove(toBlock);
            Set<Cycle> revisitCycles = revisits.get(toBlock);
            if (revisitCycles != null) {
                removeEquivalentCycles(revisitCycles, cycles);
                if (revisitCycles.isEmpty()) revisits.remove(toBlock);
            }
            mergeNewerCycles(blocked.computeIfAbsent(toBlock, b -> new HashSet<>()), cycles);
        }

        private void removeEquivalentCycles(Set<Cycle> cycles, Set<Cycle> toRemove) {
            cycles.removeAll(iterate(cycles).filter(cycle -> findEquivalent(toRemove, cycle).isPresent()).toSet());
        }

        private void retainEquivalentCycles(Set<Cycle> cycles, Set<Cycle> toRetain) {
            cycles.removeAll(iterate(cycles).filter(cycle -> findEquivalent(toRetain, cycle).isEmpty()).toSet());
        }

        private void mergeNewerCycles(Set<Cycle> cycles, Set<Cycle> toMerge) {
            iterate(toMerge).forEachRemaining(m -> {
                Optional<Cycle> equivalentCycle = findEquivalent(cycles, m);
                if (equivalentCycle.isPresent() && m.answersSeen() > equivalentCycle.get().answersSeen()) {
                    cycles.remove(equivalentCycle.get());
                    cycles.add(m);
                } else if (equivalentCycle.isEmpty()) {
                    cycles.add(m);
                }
            });
        }

        private Optional<Cycle> findEquivalent(Set<Cycle> cycles, Cycle cycle) {
            return iterate(cycles).filter(c -> c.origin().equals(cycle.origin())).first();
        }

        public Set<Cycle> cyclesToRevisit(Partial.Concludable<?> partial, int numAnswers) {
            return iterate(blockingCycles())
                    .filter(cycle -> startsHere(cycle, partial) && cycle.answersSeen() < numAnswers).toSet();
        }

        public void revisit(Set<Cycle> cycles) {
            assert !cycles.isEmpty();
            Set<Request.Factory> toRemove = new HashSet<>();
            blocked.forEach((downstream, blockers) -> {
                assert !visits.contains(downstream);
                Set<Cycle> blockersToRevisit = new HashSet<>(blockers);
                retainEquivalentCycles(blockersToRevisit, cycles);
                if (!blockersToRevisit.isEmpty()) {
                    revisits.computeIfAbsent(downstream, o -> new HashSet<>()).addAll(blockersToRevisit);
//                    mergeNewerCycles(revisits.computeIfAbsent(downstream, o -> new HashSet<>()), blockersToRevisit);
                    blockers.removeAll(blockersToRevisit);
                    if (blockers.isEmpty()) toRemove.add(downstream);
                }
            });
            toRemove.forEach(blocked::remove);
        }

        public boolean allBlockedStartHere(Partial.Concludable<?> concludable) {
            assert visits.isEmpty() && revisits.isEmpty();
            for (Set<Cycle> cycles : blocked.values()) {
                if (iterate(cycles).anyMatch(c -> !startsHere(c, concludable))) return false;
            }
            return true;
        }

        public Set<Cycle> blockersStartingElsewhere(Partial.Concludable<?> partial) {
            return iterate(blocked.values()).flatMap(
                    cycles -> iterate(cycles).filter(c -> !startsHere(c, partial))).toSet();
        }

        public static boolean startsHere(Cycle cycle, Partial.Concludable<?> partial) {
            return cycle.origin().equals(partial);
        }
    }
}
