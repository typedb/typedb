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
import com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer.Trace;
import com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer.Traced;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response.Answer;
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

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.parameters.Arguments.Query.Producer.INCREMENTAL;
import static com.vaticle.typedb.core.common.poller.Pollers.poll;
import static com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer.Traced.trace;

public abstract class Resolver<RESOLVER extends ReasonerActor<RESOLVER>> extends ReasonerActor<RESOLVER> {
    private static final Logger LOG = LoggerFactory.getLogger(Resolver.class);

    private final Map<Traced<Request>, Traced<Request>> requestRouter;
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

    public abstract void receiveVisit(Traced<Request.Visit> fromUpstream);

    protected abstract void receiveRevisit(Traced<Request.Revisit> fromUpstream);

    protected abstract void receiveAnswer(Traced<Answer> fromDownstream);

    protected abstract void receiveFail(Traced<Response.Fail> fromDownstream);

    protected void receiveCycle(Traced<Response.Cycle> fromDownstream) {
        LOG.trace("{}: received Cycle: {}", name(), fromDownstream);
        if (isTerminated()) return;
        Traced<Request> toDownstream = trace(fromDownstream.message().sourceRequest(), fromDownstream.trace());
        Traced<Request> fromUpstream = fromUpstream(toDownstream);
        cycleToUpstream(fromUpstream, fromDownstream.message().origins());
    }

    protected abstract void initialiseDownstreamResolvers(); //TODO: This method should only be required of the coordinating actors

    protected Traced<Request> fromUpstream(Traced<Request> toDownstream) {
        assert toDownstream.trace().root() != -1;
        assert requestRouter.containsKey(toDownstream);
        assert requestRouter.get(toDownstream).trace() == toDownstream.trace();
        return requestRouter.get(toDownstream);
    }

    protected Traced<Request> upstreamTracedRequest(Traced<? extends Response> response) {
        return fromUpstream(trace(response.message().sourceRequest(), response.trace()));
    }

    protected Request.Visit upstreamVisit(Traced<? extends Response> response) {
        return fromUpstream(trace(response.message().sourceRequest(), response.trace())).message().visit();
    }

    protected static Traced<Request> tracedFromUpstream(Traced<? extends Request> fromUpstream) {
        // TODO: Hacks the casting
        return Traced.trace(fromUpstream.message(), fromUpstream.trace());
    }

    protected void visitDownstream(RequestFactory downstream, Traced<Request> fromUpstream) {
        visitDownstream(downstream.createVisit(fromUpstream.trace()), fromUpstream);
    }

    protected void visitDownstream(Request.Visit visit, Traced<Request> fromUpstream) {
        LOG.trace("{} : Sending a new Visit request to downstream: {}", name(), visit);
        assert fromUpstream.trace().root() != -1;
        Traced<Request.Visit> tracedVisit = trace(visit, fromUpstream.trace());
        Traced<Request> tracedRequest = trace(visit, fromUpstream.trace());
        if (registry.resolutionTracing()) ResolutionTracer.get().visit(tracedVisit);
        requestRouter.put(tracedRequest, tracedFromUpstream(fromUpstream));
        visit.receiver().execute(actor -> actor.receiveVisit(tracedVisit));
    }

    protected void revisitDownstream(Request.Revisit revisit, Traced<Request> fromUpstream) {
        LOG.trace("{} : Sending a new Revisit request to downstream: {}", name(), revisit);
        assert fromUpstream.trace().root() != -1;
        Traced<Request.Revisit> tracedRevisit = trace(revisit, fromUpstream.trace());
        Traced<Request> tracedRequest = trace(revisit, fromUpstream.trace());  // TODO
        if (registry.resolutionTracing()) ResolutionTracer.get().revisit(tracedRevisit);
        requestRouter.put(tracedRequest, fromUpstream);
        revisit.visit().receiver().execute(actor -> actor.receiveRevisit(tracedRevisit));
    }

    protected void answerToUpstream(AnswerState answer, Traced<Request> fromUpstream) {
        assert answer.isPartial();
        Traced<Answer> response = trace(Answer.create(fromUpstream.message().visit(), answer.asPartial()), fromUpstream.trace());
        LOG.trace("{} : Sending a new Response.Answer to upstream", name());
        if (registry.resolutionTracing()) ResolutionTracer.get().responseAnswer(response);
        fromUpstream.message().visit().sender().execute(actor -> actor.receiveAnswer(response));
    }

    protected void failToUpstream(Traced<Request> fromUpstream) {
        Traced<Response.Fail> response = trace(new Response.Fail(fromUpstream.message().visit()), fromUpstream.trace());
        LOG.trace("{} : Sending a new Response.Answer to upstream", name());
        if (registry.resolutionTracing()) ResolutionTracer.get().responseExhausted(response);
        fromUpstream.message().visit().sender().execute(actor -> actor.receiveFail(response));
    }

    protected void cycleToUpstream(Traced<Request> fromUpstream, Set<Response.Cycle.Origin> cycleOrigins) {
        assert !fromUpstream.message().visit().partialAnswer().parent().isTop();
        Traced<Response.Cycle> response = trace(new Response.Cycle(fromUpstream.message().visit(), cycleOrigins), fromUpstream.trace());
        LOG.trace("{} : Sending a new Response.Cycle to upstream", name());
        if (registry.resolutionTracing()) ResolutionTracer.get().responseCycle(response);
        fromUpstream.message().visit().sender().execute(actor -> actor.receiveCycle(response));
    }

    protected void cycleToUpstream(Traced<Request> fromUpstream, int numAnswersSeen) {
        assert !fromUpstream.message().visit().partialAnswer().parent().isTop();
        Traced<Response.Cycle> response = trace(
                new Response.Cycle.Origin(fromUpstream.message().visit(), numAnswersSeen), fromUpstream.trace());
        LOG.trace("{} : Sending a new Response.Cycle to upstream", name());
        if (registry.resolutionTracing()) ResolutionTracer.get().responseCycle(response);
        fromUpstream.message().visit().sender().execute(actor -> actor.receiveCycle(response));
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

    public abstract static class RequestState {

        protected final Request.Visit fromUpstream;

        protected RequestState(Request.Visit fromUpstream) {
            this.fromUpstream = fromUpstream;
        }

        public Request.Visit fromUpstream() {
            return fromUpstream;
        }

        public abstract Optional<? extends AnswerState.Partial<?>> nextAnswer();

        public interface Exploration {

            boolean newAnswer(AnswerState.Partial<?> partial);

            DownstreamManager downstreamManager();

            boolean singleAnswerRequired();

        }

    }

    public abstract static class CachingRequestState<ANSWER> extends RequestState {

        protected final AnswerCache<ANSWER> answerCache;
        protected Poller<? extends AnswerState.Partial<?>> cacheReader;
        protected final Set<ConceptMap> deduplicationSet;

        protected CachingRequestState(Request.Visit fromUpstream, AnswerCache<ANSWER> answerCache, boolean deduplicate) {
            super(fromUpstream);
            this.answerCache = answerCache;
            this.deduplicationSet = deduplicate ? new HashSet<>() : null;
            this.cacheReader = answerCache.reader().flatMap(
                    a -> poll(toUpstream(a).filter(
                            partial -> !deduplicate || !deduplicationSet.contains(partial.conceptMap()))));
        }

        @Override
        public Optional<? extends AnswerState.Partial<?>> nextAnswer() {
            Optional<? extends AnswerState.Partial<?>> ans = cacheReader.poll();
            if (ans.isPresent() && deduplicationSet != null) deduplicationSet.add(ans.get().conceptMap());
            return ans;
        }

        protected abstract FunctionalIterator<? extends AnswerState.Partial<?>> toUpstream(ANSWER answer);

    }

    public static class DownstreamManager {
        protected final List<RequestFactory> visits;
        protected Map<RequestFactory, Set<Response.Cycle.Origin>> revisits;
        protected Map<RequestFactory, Set<Response.Cycle.Origin>> blocked;

        public DownstreamManager() {
            this.visits = new ArrayList<>();
            this.revisits = new LinkedHashMap<>();
            this.blocked = new LinkedHashMap<>();
        }

        public DownstreamManager(List<RequestFactory> visits) {
            this.visits = visits;
            this.revisits = new LinkedHashMap<>();
            this.blocked = new LinkedHashMap<>();
        }

        public Request.Visit nextVisit(Trace trace) {
            return visits.get(0).createVisit(trace);
        }

        public Request.Revisit nextRevisit(Trace trace) {
            Optional<RequestFactory> downstream = iterate(revisits.keySet()).first();
            assert downstream.isPresent();
            return downstream.get().createRevisit(trace, revisits.get(downstream.get()));
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

        public boolean contains(RequestFactory downstream) {
            return visits.contains(downstream) || revisits.containsKey(downstream) || blocked.containsKey(downstream);
        }

        public void add(RequestFactory downstream) {
            assert !(visits.contains(downstream)) : "downstream answer producer already contains this request";
            visits.add(downstream);
        }

        public void remove(RequestFactory downstream) {
            visits.remove(downstream);
            revisits.remove(downstream);
            blocked.remove(downstream);
        }

        public void clear() {
            visits.clear();
            revisits.clear();
            blocked.clear();
        }

        public Set<Response.Cycle.Origin> blockers() {
            return iterate(blocked.values()).flatMap(Iterators::iterate).toSet();
        }

        public void block(RequestFactory toBlock, Set<Response.Cycle.Origin> blockers) {
            assert !blockers.isEmpty();
            assert contains(toBlock);
            visits.remove(toBlock);
            revisits.remove(toBlock);
            blocked.computeIfAbsent(toBlock, b -> new HashSet<>()).addAll(blockers);
        }

        public void unblock(Set<Response.Cycle.Origin> cycles) {
            assert !cycles.isEmpty();
            Set<RequestFactory> toRemove = new HashSet<>();
            blocked.forEach((downstream, blockers) -> {
                assert !visits.contains(downstream);
                Set<Response.Cycle.Origin> blockersToRevisit = new HashSet<>(cycles);
                blockersToRevisit.retainAll(blockers);
                if (!blockersToRevisit.isEmpty()) {
                    this.revisits.computeIfAbsent(downstream, o -> new HashSet<>()).addAll(blockersToRevisit);
                    blockers.removeAll(blockersToRevisit);
                    if (blockers.isEmpty()) toRemove.add(downstream);
                }
            });
            toRemove.forEach(r -> blocked.remove(r));
        }
    }
}
