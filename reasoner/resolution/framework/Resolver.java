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
import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.producer.Producer;
import com.vaticle.typedb.core.concurrent.producer.Producers;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer.TraceId;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response.Answer;
import com.vaticle.typedb.core.reasoner.resolution.resolver.BoundConcludableResolver;
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

public abstract class Resolver<RESOLVER extends ReasonerActor<RESOLVER>> extends ReasonerActor<RESOLVER> {
    private static final Logger LOG = LoggerFactory.getLogger(Resolver.class);

    private final Map<Pair<Request, TraceId>, Request> requestRouter;
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

    public abstract void receiveRequest(Request fromUpstream, int iteration);

    protected abstract void receiveAnswer(Response.Answer fromDownstream, int iteration);

    protected abstract void receiveFail(Response.Fail fromDownstream, int iteration);

    protected void receiveBlocked(Response.Blocked fromDownstream, int iteration) {
        LOG.trace("{}: received Blocked: {}", name(), fromDownstream);
        if (isTerminated()) return;
        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        blockToUpstream(fromUpstream, fromDownstream.blockers(), iteration);
    }

    protected abstract void initialiseDownstreamResolvers(); //TODO: This method should only be required of the coordinating actors

    protected Request fromUpstream(Request toDownstream) {
        assert toDownstream.traceId().rootId() != -1;
        Pair<Request, TraceId> ds = new Pair<>(toDownstream, toDownstream.traceId());
        assert requestRouter.containsKey(ds);
        assert requestRouter.get(ds).traceId() == toDownstream.traceId();
        return requestRouter.get(ds);
    }

    // TODO: Rename to sendRequest or request
    protected void requestFromDownstream(Request request, Request fromUpstream, int iteration) {
        LOG.trace("{} : Sending a new answer Request to downstream: {}", name(), request);
        assert fromUpstream.traceId().rootId() != -1;
        assert request.traceId() == fromUpstream.traceId() || request.traceId().rootId() == -1;
        request = request.withTraceId(fromUpstream.traceId());
        if (registry.resolutionTracing()) ResolutionTracer.get().request(request, iteration);
        // TODO: we may overwrite if multiple identical requests are sent, when to clean up?
        Request finalRequest = request;
        requestRouter.put(new Pair<>(finalRequest, finalRequest.traceId()), fromUpstream);
        Driver<? extends Resolver<?>> receiver = finalRequest.receiver();
        receiver.execute(actor -> actor.receiveRequest(finalRequest, iteration));
    }

    // TODO: Rename to sendResponse or respond
    protected void answerToUpstream(AnswerState answer, Request fromUpstream, int iteration) {
        assert answer.isPartial();
        Answer response = Answer.create(fromUpstream, answer.asPartial());
        LOG.trace("{} : Sending a new Response.Answer to upstream", name());
        if (registry.resolutionTracing()) ResolutionTracer.get().responseAnswer(response, iteration);
        fromUpstream.sender().execute(actor -> actor.receiveAnswer(response, iteration));
    }

    protected void failToUpstream(Request fromUpstream, int iteration) {
        Response.Fail response = new Response.Fail(fromUpstream);
        LOG.trace("{} : Sending a new Response.Answer to upstream", name());
        if (registry.resolutionTracing()) ResolutionTracer.get().responseExhausted(response, iteration);
        fromUpstream.sender().execute(actor -> actor.receiveFail(response, iteration));
    }

    protected void blockToUpstream(Request fromUpstream, Set<Response.Blocked.Origin> blockers, int iteration) {
        assert !fromUpstream.partialAnswer().parent().isTop();
        Response.Blocked response = new Response.Blocked(fromUpstream, blockers);
        LOG.trace("{} : Sending a new Response.Blocked to upstream", name());
        if (registry.resolutionTracing()) ResolutionTracer.get().responseBlocked(response, iteration);
        fromUpstream.sender().execute(actor -> actor.receiveBlocked(response, iteration));
    }

    protected void blockToUpstream(Request fromUpstream, int numAnswersSeen, int iteration) {
        assert !fromUpstream.partialAnswer().parent().isTop();
        Response.Blocked response = new Response.Blocked.Origin(fromUpstream, numAnswersSeen);
        LOG.trace("{} : Sending a new Response.Blocked to upstream", name());
        if (registry.resolutionTracing()) ResolutionTracer.get().responseBlocked(response, iteration);
        fromUpstream.sender().execute(actor -> actor.receiveBlocked(response, iteration));
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

    public static class DownstreamManager {
        protected final List<Request> downstreams;

        public DownstreamManager() {
            this.downstreams = new ArrayList<>();
        }

        public DownstreamManager(List<Request> downstreams) {
            this.downstreams = downstreams;
        }

        public boolean hasDownstream() {
            return !downstreams.isEmpty();
        }

        public Request nextDownstream() {
            return downstreams.get(0);
        }

        public void addDownstream(Request request) {
            assert !(downstreams.contains(request)) : "downstream answer producer already contains this request";
            downstreams.add(request);
        }

        public void removeDownstream(Request request) {
            downstreams.remove(request);
        }

        public void clearDownstreams() {
            downstreams.clear();
        }

        public boolean contains(Request downstreamRequest) {
            return downstreams.contains(downstreamRequest);
        }

        public static class Blockable extends DownstreamManager {

            private final Map<Request, Set<Response.Blocked.Origin>> blocked;

            public Blockable(List<Request> downstreams) {
                super(downstreams);
                this.blocked = new LinkedHashMap<>();
            }

            public Blockable() {
                this.blocked = new LinkedHashMap<>();
            }

            public Optional<Request> nextUnblockedDownstream() {
                if (hasDownstream()) return Optional.of(super.nextDownstream());
                else return Optional.empty();
            }

            @Override
            public Request nextDownstream() {
                if (hasDownstream()) return super.nextDownstream();
                else {
                    Optional<Request> b = iterate(blocked.keySet()).first();
                    assert b.isPresent();
                    return b.get();
                }
            }

            public Optional<Set<Response.Blocked.Origin>> nextDownstreamBlocker() {
                if (!hasDownstream()) return Optional.empty();
                Request ds = nextDownstream();
                if (blocked.containsKey(ds)) return Optional.of(blocked.get(ds));
                else return Optional.empty();
            }

            public Set<Response.Blocked.Origin> blockers() {
                return iterate(blocked.values()).flatMap(Iterators::iterate).toSet();
            }

            public void block(Request blockedDownstream, Set<Response.Blocked.Origin> blockers) {
                blocked.computeIfAbsent(blockedDownstream, b -> new HashSet<>()).addAll(blockers);
                downstreams.remove(blockedDownstream);
            }

            public void clearBlocked(Driver<BoundConcludableResolver> blockSender, int numAnswersProduced) {
                blocked.forEach((downstream, blockers) -> {
                    Set<Response.Blocked.Origin> newBlockers = iterate(blockers)
                            .filter(blocker -> !blocker.sender().equals(blockSender) || blocker.numAnswersSeen() < numAnswersProduced)
                            .toSet();
                    blockers.clear();
                    blockers.addAll(newBlockers);
//                    iterate(blockers).filter(blocker -> blocker.sender().equals(blockSender) || blocker.numAnswersSeen() == numAnswersProduced).remove(); // TODO: Unsupported
                });
            }

            public boolean blocksAll(Driver<BoundConcludableResolver> blockSender) {
                return iterate(blocked.values())
                        .filter(blockers -> iterate(blockers)
                                .filter(blocker -> blocker.sender().equals(blockSender))
                                .first()
                                .isEmpty())
                        .first()
                        .isPresent();
            }
        }
    }
}
