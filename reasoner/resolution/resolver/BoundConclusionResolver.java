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

import com.vaticle.typedb.common.collection.Pair;
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
    private final Map<Materialiser.Request, Pair<Request.Visit, Integer>> materialiserRequestRouter;

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
    public void receiveVisit(Request.Visit fromUpstream, int iteration) {
        LOG.trace("{}: received Visit: {}", name(), fromUpstream);
        if (isTerminated()) return;
        if (fromUpstream.isToSubsumed()) {
            assert fromUpstream.partialAnswer().conceptMap().equals(bounds);
            receiveSubsumedRequest(fromUpstream.asToSubsumed(), iteration);
        } else if (fromUpstream.isToSubsumer()) {
            receiveSubsumerRequest(fromUpstream.asToSubsumer(), iteration);
        } else {
            assert fromUpstream.partialAnswer().conceptMap().equals(bounds);
            receiveDirectRequest(fromUpstream, iteration);
        }
    }

    private void receiveSubsumedRequest(Request.Visit.ToSubsumed fromUpstream, int iteration) {
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

    private void receiveSubsumerRequest(Request.Visit.ToSubsumer fromUpstream, int iteration) {
        throw TypeDBException.of(ILLEGAL_STATE);
//        sendAnswerOrFail(fromUpstream, iteration, requestStates.computeIfAbsent(
//                fromUpstream, request -> new SubsumerRequestState(request, cache, iteration)));
    }

    private void receiveDirectRequest(Request.Visit fromUpstream, int iteration) {
        ConclusionRequestState<? extends Concludable<?>> requestState = requestStates.computeIfAbsent(
                fromUpstream, r -> createRequestState(fromUpstream, iteration));
        if (!sendAnswerOrSearchDownstream(fromUpstream, requestState, iteration)) {
            if (requestState.waitedMaterialisations().waiting()) {
                requestState.waitedMaterialisations().addWaitingVisit(fromUpstream, iteration);
            } else {
                cycleOrFail(fromUpstream, requestState, iteration);
            }
        }
    }

    @Override
    protected void receiveRevisit(Request.Revisit fromUpstream, int iteration) {
        ConclusionRequestState<? extends Concludable<?>> requestState = requestStates.get(fromUpstream.visit());
        requestState.downstreamManager().unblock(fromUpstream.cycles());
        if (!sendAnswerOrSearchDownstream(fromUpstream.visit(), requestState, iteration)) {
            if (requestState.waitedMaterialisations().waiting()) {
                requestState.waitedMaterialisations().addWaitingRevisit(fromUpstream, iteration);
            } else {
                cycleOrFail(fromUpstream.visit(), requestState, iteration);
            }
        }
    }


    @Override
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        LOG.trace("{}: received Answer: {}", name(), fromDownstream);
        if (isTerminated()) return;
        Request.Visit toDownstream = fromDownstream.sourceRequest();
        Request.Visit fromUpstream = fromUpstream(toDownstream);
        ConclusionRequestState<? extends Concludable<?>> requestState = this.requestStates.get(fromUpstream);
        if (!requestState.isComplete()) {
            Materialiser.Request request = Materialiser.Request.create(
                    driver(), registry.materialiser(), fromUpstream.traceId(), conclusion, fromDownstream.answer());
            requestFromMaterialiser(request, fromUpstream, iteration);
            requestState.waitedMaterialisations().increment();
        } else {
            if (!sendAnswerOrSearchDownstream(fromUpstream, requestState, iteration)) {
                cycleOrFail(fromUpstream, requestState, iteration);
            }
        }
    }

    @Override
    protected void receiveCycle(Response.Cycle fromDownstream, int iteration) {
        LOG.trace("{}: received Cycle: {}", name(), fromDownstream);
        if (isTerminated()) return;
        Downstream downstream = Downstream.of(fromDownstream.sourceRequest());
        Request.Visit fromUpstream = fromUpstream(fromDownstream.sourceRequest());
        ConclusionRequestState<? extends Concludable<?>> requestState = this.requestStates.get(fromUpstream);
        if (requestState.waitedMaterialisations().waiting()) {
            requestState.waitedMaterialisations().addWaitingCycle(fromDownstream, iteration);
        } else {
            if (requestState.downstreamManager().contains(downstream)) {
                requestState.downstreamManager().block(downstream, fromDownstream.origins());
            }
            if (!sendAnswerOrSearchDownstream(fromUpstream, requestState, iteration)) {
                cycleOrFail(fromUpstream, requestState, iteration);
            }
        }
    }

    private void requestFromMaterialiser(Materialiser.Request request, Request.Visit fromUpstream, int iteration) {
        if (registry.resolutionTracing()) ResolutionTracer.get().visit(request, iteration);
        materialiserRequestRouter.put(request, new Pair<>(fromUpstream, iteration));
        registry.materialiser().execute(actor -> actor.receiveRequest(request));
    }

    public void receiveMaterialisation(Materialiser.Response response) {
        if (isTerminated()) return;
        Materialiser.Request toDownstream = response.sourceRequest();
        Pair<Request.Visit, Integer> fromUpstream = fromUpstream(toDownstream);
        ConclusionRequestState<? extends Concludable<?>> requestState = this.requestStates.get(fromUpstream.first());
        LOG.trace("{}: received materialisation response: {}", name(), response);
        Optional<Map<Identifier.Variable, Concept>> materialisation = response.materialisation();
        materialisation.ifPresent(m -> requestState.newMaterialisation(response.partialAnswer(), m));
        Request.Visit fromUpstream1 = fromUpstream.first();
        int iteration = fromUpstream.second();
        if (!sendAnswerOrSearchDownstream(fromUpstream1, requestState, iteration)) {
            cycleOrFail(fromUpstream1, requestState, iteration);
        }
        ConclusionRequestState.WaitedMaterialisations state = requestState.waitedMaterialisations();
        state.decrement();
        if (!state.waiting()) {
            while (state.hasNextWaitingVisit()) {
                Pair<Request.Visit, Integer> visit = state.nextWaitingVisit();
                receiveVisit(visit.first(), visit.second());
            }
            while (state.hasNextWaitingCycle()) {
                Pair<Response.Cycle, Integer> cycle = state.nextWaitingCycle();
                receiveCycle(cycle.first(), cycle.second());
            }
            while (state.hasNextWaitingRevisit()) {
                Pair<Request.Revisit, Integer> revisit = state.nextWaitingRevisit();
                receiveRevisit(revisit.first(), revisit.second());
            }
        }
    }

    protected Pair<Request.Visit, Integer> fromUpstream(Materialiser.Request toDownstream) {
        assert materialiserRequestRouter.containsKey(toDownstream);
        return materialiserRequestRouter.remove(toDownstream);
    }

    private boolean sendAnswerOrSearchDownstream(Request.Visit fromUpstream, ConclusionRequestState<?> requestState, int iteration) {
        Optional<? extends AnswerState.Partial<?>> upstreamAnswer = requestState.nextAnswer();
        if (upstreamAnswer.isPresent()) {
            answerToUpstream(upstreamAnswer.get(), fromUpstream, iteration);
        } else if (!requestState.isComplete() && requestState.downstreamManager().hasNextVisit()) {
            visitDownstream(requestState.downstreamManager().nextVisit(fromUpstream), fromUpstream, iteration);
        } else if (!requestState.isComplete() && requestState.downstreamManager().hasNextRevisit()) {
            revisitDownstream(requestState.downstreamManager().nextRevisit(fromUpstream), fromUpstream, iteration);
        } else {
            return false;
        }
        return true;
    }

    private void cycleOrFail(Request.Visit fromUpstream, ConclusionRequestState<?> requestState, int iteration) {
        if (requestState.downstreamManager().hasNextBlocked()) {
            cycleToUpstream(fromUpstream, requestState.downstreamManager().blockers(), iteration);
        } else {
            requestState.setComplete();
            failToUpstream(fromUpstream, iteration);
        }
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        LOG.trace("{}: received Fail: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request.Visit toDownstream = fromDownstream.sourceRequest();
        Request.Visit fromUpstream = fromUpstream(toDownstream);
        ConclusionRequestState<?> requestState = this.requestStates.get(fromUpstream);

        if (iteration < requestState.iteration()) {
            // short circuit old iteration fail messages to upstream
            failToUpstream(fromUpstream, iteration);
            return;
        }

        requestState.downstreamManager().remove(Downstream.of(fromDownstream.sourceRequest()));
        if (!sendAnswerOrSearchDownstream(fromUpstream, requestState, iteration)) {
            cycleOrFail(fromUpstream, requestState, iteration);
        }
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    private ConclusionRequestState<?> createRequestState(Request.Visit fromUpstream, int iteration) {
        LOG.debug("{}: Creating a new ConclusionResponse for request: {}", name(), fromUpstream);

        ConclusionRequestState<?> requestState;
        if (fromUpstream.partialAnswer().asConclusion().isExplain()) {
            requestState = new ConclusionRequestState.Explain(fromUpstream, iteration, conditionDownstreams(fromUpstream));
        } else if (fromUpstream.partialAnswer().asConclusion().isMatch()) {
            requestState = new ConclusionRequestState.Match(fromUpstream, iteration, conditionDownstreams(fromUpstream));
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

    private static abstract class ConclusionRequestState<CONCLUDABLE extends Concludable<?>> extends RequestState {

        private final DownstreamManager downstreamManager;
        private final WaitedMaterialisations waitedMaterialisations;
        private boolean complete;
        protected FunctionalIterator<CONCLUDABLE> materialisations;

        protected ConclusionRequestState(Request.Visit fromUpstream, int iteration, List<Downstream> conditionDownstreams) {
            super(fromUpstream, iteration);
            this.downstreamManager = new DownstreamManager(conditionDownstreams);
            this.materialisations = Iterators.empty();
            this.complete = false;
            this.waitedMaterialisations = new WaitedMaterialisations();
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

        private WaitedMaterialisations waitedMaterialisations() {
            return waitedMaterialisations;
        }

        private static class WaitedMaterialisations {
            private int waitedMaterialisations;
            private final Deque<Pair<Request.Visit, Integer>> waitingVisits;
            private final Deque<Pair<Request.Revisit, Integer>> waitingRevisits;
            private final Deque<Pair<Response.Cycle, Integer>> waitingCycles;

            private WaitedMaterialisations() {
                this.waitedMaterialisations = 0;
                this.waitingVisits = new ArrayDeque<>();
                this.waitingRevisits = new ArrayDeque<>();
                this.waitingCycles = new ArrayDeque<>();
            }

            public void addWaitingVisit(Request.Visit fromUpstream, int iteration) {
                waitingVisits.add(new Pair<>(fromUpstream, iteration));
            }

            public void addWaitingRevisit(Request.Revisit fromUpstream, int iteration) {
                waitingRevisits.add(new Pair<>(fromUpstream, iteration));
            }

            public void addWaitingCycle(Response.Cycle fromDownstream, int iteration) {
                waitingCycles.add(new Pair<>(fromDownstream, iteration));
            }

            public boolean hasNextWaitingVisit() {
                return waitingVisits.size() > 0;
            }

            public Pair<Request.Visit, Integer> nextWaitingVisit() {
                return waitingVisits.pop();
            }

            public boolean hasNextWaitingRevisit() {
                return waitingRevisits.size() > 0;
            }

            public Pair<Request.Revisit, Integer> nextWaitingRevisit() {
                return waitingRevisits.pop();
            }

            public boolean hasNextWaitingCycle() {
                return waitingCycles.size() > 0;
            }

            public Pair<Response.Cycle, Integer> nextWaitingCycle() {
                return waitingCycles.pop();
            }

            public boolean waiting() {
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

            private Match(Request.Visit fromUpstream, int iteration, List<Downstream> conditionDownstreams) {
                super(fromUpstream, iteration, conditionDownstreams);
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

            private Explain(Request.Visit fromUpstream, int iteration, List<Downstream> conditionDownstreams) {
                super(fromUpstream, iteration, conditionDownstreams);
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
