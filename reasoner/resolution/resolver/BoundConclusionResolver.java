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

package com.vaticle.typedb.core.reasoner.resolution.resolver;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial.Concludable;
import com.vaticle.typedb.core.reasoner.resolution.framework.Downstream;
import com.vaticle.typedb.core.reasoner.resolution.framework.Materialiser;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.RequestState;
import com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer;
import com.vaticle.typedb.core.reasoner.resolution.framework.Resolver;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response;
import com.vaticle.typedb.core.traversal.GraphTraversal;
import com.vaticle.typedb.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class BoundConclusionResolver extends Resolver<BoundConclusionResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(BoundConclusionResolver.class);
    private final ConceptMap bounds;
    private final Rule.Conclusion conclusion;
    private final Map<Request.Visit, ConclusionRequestState<? extends Concludable<?>>> requestStates;
    private final Map<Materialiser.Request, Request.Visit> materialiserRequestRouter;

    public BoundConclusionResolver(Driver<BoundConclusionResolver> driver, Rule.Conclusion conclusion,
                                   ConceptMap bounds, ResolverRegistry registry) {
        super(driver, BoundConclusionResolver.class.getSimpleName() + "(" + conclusion +
                ", bounds: " + bounds.toString() + ")", registry);
        this.bounds = bounds;
        this.conclusion = conclusion;
        this.requestStates = new HashMap<>();
        this.materialiserRequestRouter = new HashMap<>();
    }

    @Override
    public void receiveVisit(Request.Visit fromUpstream) {
        LOG.trace("{}: received Visit: {}", name(), fromUpstream);
        if (isTerminated()) return;
        if (fromUpstream.isToSubsumed()) {
            assert fromUpstream.partialAnswer().conceptMap().equals(bounds);
            receiveSubsumedRequest(fromUpstream.asToSubsumed());
        } else if (fromUpstream.isToSubsumer()) {
            receiveSubsumerRequest(fromUpstream.asToSubsumer());
        } else {
            assert fromUpstream.partialAnswer().conceptMap().equals(bounds);
            receiveDirectRequest(fromUpstream);
        }
    }

    private void receiveSubsumedRequest(Request.Visit.ToSubsumed fromUpstream) {
        throw TypeDBException.of(ILLEGAL_STATE);
//        RequestState requestState = requestStates.computeIfAbsent(
//                fromUpstream, request -> new BoundRequestState(request, cache, iteration));
//        if (cache.sourceExhausted()) {
//            sendAnswerOrFail(fromUpstream, iteration, requestState);
//        } else {
//            cache.clearSource();
//            Optional<? extends AnswerState.Partial<?>> upstreamAnswer;
//            upstreamAnswer = requestState.nextAnswer();
//            if (upstreamAnswer.isPresent()) {
//                answerToUpstream(upstreamAnswer.get(), fromUpstream, iteration);
//            } else {
//                requestFromSubsumer(fromUpstream, iteration);
//            }
//        }
    }

    private void receiveSubsumerRequest(Request.Visit.ToSubsumer fromUpstream) {
        throw TypeDBException.of(ILLEGAL_STATE);
//        sendAnswerOrFail(fromUpstream, iteration, requestStates.computeIfAbsent(
//                fromUpstream, request -> new SubsumerRequestState(request, cache, iteration)));
    }

    private void receiveDirectRequest(Request.Visit fromUpstream) {
        ConclusionRequestState<? extends Concludable<?>> requestState = requestStates.computeIfAbsent(
                fromUpstream, r -> createRequestState(fromUpstream));
        if (requestState.materialisationsCounter().nonZero()) {
            requestState.replayBuffer().addVisit(fromUpstream);
        } else {
            if (!sendAnswerOrSearchDownstream(fromUpstream, requestState)) {
                cycleOrFail(fromUpstream, requestState);
            }
        }
    }

    @Override
    protected void receiveRevisit(Request.Revisit fromUpstream) {
        ConclusionRequestState<? extends Concludable<?>> requestState = requestStates.get(fromUpstream.visit());
        if (requestState.materialisationsCounter().nonZero()) {
            requestState.replayBuffer().addRevisit(fromUpstream);
        } else {
            requestState.downstreamManager().unblock(fromUpstream.cycles());
            if (!sendAnswerOrSearchDownstream(fromUpstream.visit(), requestState)) {
                cycleOrFail(fromUpstream.visit(), requestState);
            }
        }
    }


    @Override
    protected void receiveAnswer(Response.Answer fromDownstream) {
        LOG.trace("{}: received Answer: {}", name(), fromDownstream);
        if (isTerminated()) return;
        Request.Visit toDownstream = fromDownstream.sourceRequest();
        Request.Visit fromUpstream = fromUpstream(toDownstream);
        ConclusionRequestState<? extends Concludable<?>> requestState = this.requestStates.get(fromUpstream);
        if (!requestState.isComplete()) {
            Materialiser.Request request = Materialiser.Request.create(
                    driver(), registry.materialiser(), fromUpstream.traceId(), conclusion, fromDownstream.answer());
            requestFromMaterialiser(request, fromUpstream);
            requestState.materialisationsCounter().increment();
        } else {
            if (!sendAnswerOrSearchDownstream(fromUpstream, requestState)) {
                cycleOrFail(fromUpstream, requestState);
            }
        }
    }

    @Override
    protected void receiveCycle(Response.Cycle fromDownstream) {
        LOG.trace("{}: received Cycle: {}", name(), fromDownstream);
        if (isTerminated()) return;
        Downstream downstream = Downstream.of(fromDownstream.sourceRequest());
        Request.Visit fromUpstream = fromUpstream(fromDownstream.sourceRequest());
        ConclusionRequestState<? extends Concludable<?>> requestState = this.requestStates.get(fromUpstream);
        if (requestState.materialisationsCounter().nonZero()) {
            requestState.replayBuffer().addCycle(fromDownstream);
        } else {
            if (requestState.downstreamManager().contains(downstream)) {
                requestState.downstreamManager().block(downstream, fromDownstream.origins());
            }
            if (!sendAnswerOrSearchDownstream(fromUpstream, requestState)) {
                cycleOrFail(fromUpstream, requestState);
            }
        }
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream) {
        LOG.trace("{}: received Fail: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request.Visit toDownstream = fromDownstream.sourceRequest();
        Request.Visit fromUpstream = fromUpstream(toDownstream);
        ConclusionRequestState<?> requestState = this.requestStates.get(fromUpstream);

        if (requestState.materialisationsCounter().nonZero()) {
            requestState.replayBuffer().addFail(fromDownstream);
        } else {
            requestState.downstreamManager().remove(Downstream.of(fromDownstream.sourceRequest()));
            if (!sendAnswerOrSearchDownstream(fromUpstream, requestState)) {
                cycleOrFail(fromUpstream, requestState);
            }
        }
    }

    private void requestFromMaterialiser(Materialiser.Request request, Request.Visit fromUpstream) {
        if (registry.resolutionTracing()) ResolutionTracer.get().visit(request);
        materialiserRequestRouter.put(request, fromUpstream);
        registry.materialiser().execute(actor -> actor.receiveRequest(request));
    }

    public void receiveMaterialisation(Materialiser.Response response) {
        if (isTerminated()) return;
        Materialiser.Request toDownstream = response.sourceRequest();
        Request.Visit fromUpstream = fromUpstream(toDownstream);
        ConclusionRequestState<? extends Concludable<?>> requestState = this.requestStates.get(fromUpstream);
        LOG.trace("{}: received materialisation response: {}", name(), response);
        Optional<Map<Identifier.Variable, Concept>> materialisation = response.materialisation();
        materialisation.ifPresent(m -> requestState.newMaterialisation(response.partialAnswer(), m));
        if (!sendAnswerOrSearchDownstream(fromUpstream, requestState)) {
            cycleOrFail(fromUpstream, requestState);
        }
        requestState.materialisationsCounter().decrement();
        if (!requestState.materialisationsCounter().nonZero()) requestState.replayBuffer().replay();
    }

    protected Request.Visit fromUpstream(Materialiser.Request toDownstream) {
        assert materialiserRequestRouter.containsKey(toDownstream);
        return materialiserRequestRouter.remove(toDownstream);
    }

    private boolean sendAnswerOrSearchDownstream(Request.Visit fromUpstream, ConclusionRequestState<?> requestState) {
        Optional<? extends AnswerState.Partial<?>> upstreamAnswer = requestState.nextAnswer();
        if (upstreamAnswer.isPresent()) {
            answerToUpstream(upstreamAnswer.get(), fromUpstream);
        } else if (!requestState.isComplete() && requestState.downstreamManager().hasNextVisit()) {
            visitDownstream(requestState.downstreamManager().nextVisit(fromUpstream), fromUpstream);
        } else if (!requestState.isComplete() && requestState.downstreamManager().hasNextRevisit()) {
            revisitDownstream(requestState.downstreamManager().nextRevisit(fromUpstream), fromUpstream);
        } else {
            return false;
        }
        return true;
    }

    private void cycleOrFail(Request.Visit fromUpstream, ConclusionRequestState<?> requestState) {
        if (requestState.downstreamManager().hasNextBlocked()) {
            cycleToUpstream(fromUpstream, requestState.downstreamManager().blockers());
        } else {
            requestState.setComplete();
            failToUpstream(fromUpstream);
        }
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    private ConclusionRequestState<?> createRequestState(Request.Visit fromUpstream) {
        LOG.debug("{}: Creating a new ConclusionResponse for request: {}", name(), fromUpstream);

        ConclusionRequestState<?> requestState;
        if (fromUpstream.partialAnswer().asConclusion().isExplain()) {
            requestState = new ConclusionRequestState.Explain(fromUpstream, conditionDownstreams(fromUpstream), new ReplayBuffer());
        } else if (fromUpstream.partialAnswer().asConclusion().isMatch()) {
            requestState = new ConclusionRequestState.Match(fromUpstream, conditionDownstreams(fromUpstream), new ReplayBuffer());
        } else {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        return requestState;
    }

    private List<Downstream> conditionDownstreams(Request.Visit fromUpstream) {
        // TODO: Can there be more than one downstream Condition? If not reduce this return type from List to single
        // we do a extra traversal to expand the partial answer if we already have the concept that is meant to be generated
        // and if there's extra variables to be populated
        AnswerState.Partial.Conclusion<?, ?> partialAnswer = fromUpstream.partialAnswer().asConclusion();
        assert fromUpstream.partialAnswer().isConclusion();
        assert conclusion.retrievableIds().containsAll(partialAnswer.conceptMap().concepts().keySet());

        List<Downstream> downstreams = new ArrayList<>();
        if (conclusion.generating().isPresent() && conclusion.retrievableIds().size() > partialAnswer.conceptMap().concepts().size() &&
                partialAnswer.conceptMap().concepts().containsKey(conclusion.generating().get().id())) {
            candidateAnswers(partialAnswer).forEachRemaining(answer -> downstreams.add(
                    Downstream.create(driver(), registry.conditionResolver(conclusion.rule()), answer)));
        } else {
            Set<Identifier.Variable.Retrievable> named = iterate(conclusion.retrievableIds()).filter(Identifier::isName).toSet();
            downstreams.add(Downstream.create(driver(), registry.conditionResolver(conclusion.rule()), partialAnswer.toDownstream(named)));
        }
        return downstreams;
    }

    private FunctionalIterator<AnswerState.Partial.Compound<?, ?>> candidateAnswers(
            AnswerState.Partial.Conclusion<?, ?> partialAnswer) {
        GraphTraversal.Thing traversal = boundTraversal(conclusion.conjunction().traversal(), partialAnswer.conceptMap());
        Set<Identifier.Variable.Retrievable> named = iterate(conclusion.retrievableIds()).filter(Identifier::isName).toSet();
        return registry.traversalEngine().iterator(traversal)
                .map(v -> registry.conceptManager().conceptMap(v))
                .map(ans -> partialAnswer.extend(ans).toDownstream(named));
    }

    private class ReplayBuffer {
        private final Deque<Class<?>> replayOrder;
        private final Deque<Request.Visit> visits;
        private final Deque<Request.Revisit> revisits;
        private final Deque<Response.Cycle> cycles;
        private final Deque<Response.Fail> fails;

        private ReplayBuffer() {
            this.replayOrder = new ArrayDeque<>();
            this.visits = new ArrayDeque<>();
            this.revisits = new ArrayDeque<>();
            this.cycles = new ArrayDeque<>();
            this.fails = new ArrayDeque<>();
        }

        public void replay() {
            while (replayOrder.size() > 0) {
                Class<?> nextMessageType = replayOrder.pop();
                if (nextMessageType.equals(Request.Visit.class)) receiveVisit(visits.pop());
                else if (nextMessageType.equals(Response.Cycle.class)) receiveCycle(cycles.pop());
                else if (nextMessageType.equals(Request.Revisit.class)) receiveRevisit(revisits.pop());
                else if (nextMessageType.equals(Response.Fail.class)) receiveFail(fails.pop());
            }
        }

        public void addVisit(Request.Visit fromUpstream) {
            visits.add(fromUpstream);
            replayOrder.add(Request.Visit.class);
        }

        public void addRevisit(Request.Revisit fromUpstream) {
            revisits.add(fromUpstream);
            replayOrder.add(Request.Revisit.class);
        }

        public void addCycle(Response.Cycle fromDownstream) {
            cycles.add(fromDownstream);
            replayOrder.add(Response.Cycle.class);
        }

        public void addFail(Response.Fail fromDownstream) {
            fails.add(fromDownstream);
            replayOrder.add(Response.Fail.class);
        }
    }

    private static abstract class ConclusionRequestState<CONCLUDABLE extends Concludable<?>> extends RequestState {

        private final DownstreamManager downstreamManager;
        private final MaterialisationCounter materialisationsCounter;
        private final ReplayBuffer replayBuffer;
        private boolean complete;
        protected FunctionalIterator<CONCLUDABLE> materialisations;

        protected ConclusionRequestState(Request.Visit fromUpstream, List<Downstream> conditionDownstreams, ReplayBuffer replayBuffer) {
            super(fromUpstream);
            this.downstreamManager = new DownstreamManager(conditionDownstreams);
            this.materialisations = Iterators.empty();
            this.complete = false;
            this.materialisationsCounter = new MaterialisationCounter();
            this.replayBuffer = replayBuffer;
        }

        public DownstreamManager downstreamManager() {
            return downstreamManager;
        }

        public boolean isComplete() {
            // TODO: Placeholder for re-introducing caching
            return complete;
        }

        public void setComplete() {
            // TODO: Placeholder for re-introducing caching. Should only be used once Conclusion caching works in
            //  recursive settings
            complete = true;
        }

        protected abstract FunctionalIterator<CONCLUDABLE> toUpstream(AnswerState.Partial<?> fromDownstream,
                                                                      Map<Identifier.Variable, Concept> answer);

        public void newMaterialisation(AnswerState.Partial<?> fromDownstream,
                                       Map<Identifier.Variable, Concept> materialisation) {
            this.materialisations = this.materialisations.link(toUpstream(fromDownstream, materialisation));
        }

        private MaterialisationCounter materialisationsCounter() {
            return materialisationsCounter;
        }

        public ReplayBuffer replayBuffer() {
            return replayBuffer;
        }

        private static class MaterialisationCounter {
            private int waitedMaterialisations;

            MaterialisationCounter() {
                this.waitedMaterialisations = 0;
            }

            public boolean nonZero() {
                return waitedMaterialisations > 0;
            }

            public void decrement() {
                waitedMaterialisations -= 1;
            }

            public void increment() {
                waitedMaterialisations += 1;
            }
        }

        private static class Match extends ConclusionRequestState<Concludable.Match<?>> {

            private final Set<ConceptMap> deduplicationSet;

            private Match(Request.Visit fromUpstream, List<Downstream> conditionDownstreams, ReplayBuffer replayBuffer) {
                super(fromUpstream, conditionDownstreams, replayBuffer);
                this.deduplicationSet = new HashSet<>();
            }

            @Override
            protected FunctionalIterator<Concludable.Match<?>> toUpstream(AnswerState.Partial<?> fromDownstream,
                                                                          Map<Identifier.Variable, Concept> answer) {
                return fromDownstream.asConclusion().asMatch().aggregateToUpstream(answer);
            }

            @Override
            public Optional<Concludable.Match<?>> nextAnswer() {
                if (!materialisations.hasNext()) return Optional.empty();
                while (materialisations.hasNext()) {
                    Concludable.Match<?> ans = materialisations.next();
                    if (!deduplicationSet.contains(ans.conceptMap())) {
                        deduplicationSet.add(ans.conceptMap());
                        return Optional.of(ans);
                    }
                }
                return Optional.empty();
            }
        }

        private static class Explain extends ConclusionRequestState<Concludable.Explain> {

            private Explain(Request.Visit fromUpstream, List<Downstream> conditionDownstreams, ReplayBuffer replayBuffer) {
                super(fromUpstream, conditionDownstreams, replayBuffer);
            }

            @Override
            protected FunctionalIterator<Concludable.Explain> toUpstream(AnswerState.Partial<?> fromDownstream,
                                                                         Map<Identifier.Variable, Concept> answer) {
                return fromDownstream.asConclusion().asExplain().aggregateToUpstream(answer);
            }

            @Override
            public Optional<Concludable.Explain> nextAnswer() {
                if (!materialisations.hasNext()) return Optional.empty();
                return Optional.of(materialisations.next());
            }
        }
    }
}
