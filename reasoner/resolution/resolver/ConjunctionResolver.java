/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.reasoner.resolution.resolver;

import grakn.core.common.iterator.Iterators;
import grakn.core.concept.ConceptManager;
import grakn.core.concurrent.actor.Actor;
import grakn.core.logic.LogicManager;
import grakn.core.logic.resolvable.Concludable;
import grakn.core.logic.resolvable.Negated;
import grakn.core.logic.resolvable.Resolvable;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Negation;
import grakn.core.reasoner.resolution.Planner;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial.Mapped;
import grakn.core.reasoner.resolution.answer.Mapping;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier.Variable.Retrievable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static grakn.core.common.iterator.Iterators.iterate;

public abstract class ConjunctionResolver<T extends ConjunctionResolver<T>> extends Resolver<T> {

    private static final Logger LOG = LoggerFactory.getLogger(ConjunctionResolver.class);

    private final LogicManager logicMgr;
    private final Planner planner;
    final Actor<ResolutionRecorder> resolutionRecorder;
    final grakn.core.pattern.Conjunction conjunction;
    final Set<Resolvable<?>> resolvables;
    final Set<Negated> negateds;
    final Plans plans;
    final Map<Resolvable<?>, ResolverRegistry.MappedResolver> downstreamResolvers;
    final Map<Request, ResponseProducer> responseProducers;
    private boolean isInitialised;

    public ConjunctionResolver(Actor<T> self, String name, grakn.core.pattern.Conjunction conjunction,
                               Actor<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry,
                               TraversalEngine traversalEngine, ConceptManager conceptMgr, LogicManager logicMgr,
                               Planner planner, boolean explanations) {
        super(self, name, registry, traversalEngine, conceptMgr, explanations);
        this.logicMgr = logicMgr;
        this.resolutionRecorder = resolutionRecorder;
        this.planner = planner;
        this.conjunction = conjunction;
        this.resolvables = new HashSet<>();
        this.negateds = new HashSet<>();
        this.plans = new Plans();
        this.responseProducers = new HashMap<>();
        this.isInitialised = false;
        this.downstreamResolvers = new HashMap<>();
    }

    protected abstract void nextAnswer(Request fromUpstream, ResponseProducer responseProducer, int iteration);

    protected abstract Optional<AnswerState> toUpstreamAnswer(Partial<?> fromDownstream);

    protected boolean mustOffset() { return false; }

    protected void offsetOccurred() {}

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);

        if (!isInitialised) initialiseDownstreamResolvers();

        ResponseProducer responseProducer = mayUpdateAndGetResponseProducer(fromUpstream, iteration);
        if (iteration < responseProducer.iteration()) {
            // short circuit if the request came from a prior iteration
            failToUpstream(fromUpstream, iteration);
        } else {
            assert iteration == responseProducer.iteration();
            nextAnswer(fromUpstream, responseProducer, iteration);
        }
    }

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        LOG.trace("{}: received Answer: {}", name(), fromDownstream);

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);

        Plans.Plan plan = plans.get(fromUpstream.partialAnswer().conceptMap().concepts().keySet());

        // TODO: this is a bit of a hack, we want requests to a negation to be "single use", otherwise we can end up in an infinite loop
        // TODO: where the request to the negation never gets removed and we constantly re-request from it!
        // TODO: this could be either implemented with a different response type: FinalAnswer, or splitting Request into ReusableRequest vs SingleRequest
        if (plan.get(toDownstream.planIndex()).isNegated()) responseProducer.removeDownstreamProducer(toDownstream);

        if (plan.isLast(fromDownstream.planIndex())) {
            Optional<AnswerState> answer = toUpstreamAnswer(fromDownstream.answer());
            if (answer.isPresent() && !responseProducer.hasProduced(answer.get().conceptMap())) {
                responseProducer.recordProduced(answer.get().conceptMap());
                if (mustOffset()) {
                    offsetOccurred();
                    nextAnswer(fromUpstream, responseProducer, iteration);
                    return;
                }
                answerToUpstream(answer.get(), fromUpstream, iteration);
            } else {
                nextAnswer(fromUpstream, responseProducer, iteration);
            }
        } else {
            int nextResolverIndex = fromDownstream.planIndex() + 1;
            ResolverRegistry.MappedResolver nextPlannedDownstream = downstreamResolvers.get(plan.get(nextResolverIndex));
            Mapped downstream = fromDownstream.answer().mapToDownstream(Mapping.of(nextPlannedDownstream.mapping()));
            Request downstreamRequest = Request.create(self(), nextPlannedDownstream.resolver(), downstream, nextResolverIndex);
            responseProducer.addDownstreamProducer(downstreamRequest);
            requestFromDownstream(downstreamRequest, fromUpstream, iteration);
        }
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        LOG.trace("{}: Receiving Exhausted: {}", name(), fromDownstream);
        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);

        if (iteration < responseProducer.iteration()) {
            // short circuit old iteration failed messages to upstream
            failToUpstream(fromUpstream, iteration);
            return;
        }

        responseProducer.removeDownstreamProducer(fromDownstream.sourceRequest());
        nextAnswer(fromUpstream, responseProducer, iteration);
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        LOG.debug("{}: initialising downstream resolvers", name());
        Set<Concludable> concludables = Iterators.iterate(Concludable.create(conjunction))
                .filter(c -> c.getApplicableRules(conceptMgr, logicMgr).hasNext()).toSet();
        Set<grakn.core.logic.resolvable.Retrievable> retrievables = grakn.core.logic.resolvable.Retrievable.extractFrom(conjunction, concludables);
        resolvables.addAll(concludables);
        resolvables.addAll(retrievables);
        iterate(resolvables).forEachRemaining(resolvable -> downstreamResolvers.put(resolvable,
                                                                                    registry.registerResolvable(resolvable)));
        for (Negation negation : conjunction.negations()) {
            Negated negated = new Negated(negation);
            downstreamResolvers.put(negated, registry.negated(negated, conjunction));
            negateds.add(negated);
        }
        isInitialised = true;
    }

    private ResponseProducer mayUpdateAndGetResponseProducer(Request fromUpstream, int iteration) {
        if (!responseProducers.containsKey(fromUpstream)) {
            responseProducers.put(fromUpstream, responseProducerCreate(fromUpstream, iteration));
        } else {
            ResponseProducer responseProducer = responseProducers.get(fromUpstream);
            assert responseProducer.iteration() == iteration ||
                    responseProducer.iteration() + 1 == iteration;

            if (responseProducer.iteration() + 1 == iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                ResponseProducer responseProducerNextIter = responseProducerReiterate(fromUpstream, responseProducer, iteration);
                responseProducers.put(fromUpstream, responseProducerNextIter);
            }
        }
        return responseProducers.get(fromUpstream);
    }

    @Override
    protected ResponseProducer responseProducerCreate(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating a new ResponseProducer for request: {}", name(), fromUpstream);
        Plans.Plan plan = plans.getOrCreate(fromUpstream.partialAnswer().conceptMap().concepts().keySet(), resolvables, negateds);
        assert !plan.isEmpty();

        ResponseProducer responseProducer = new ResponseProducer(Iterators.empty(), iteration);
        Mapped downstream = fromUpstream.partialAnswer()
                .mapToDownstream(Mapping.of(downstreamResolvers.get(plan.get(0)).mapping()));
        Request toDownstream = Request.create(self(), downstreamResolvers.get(plan.get(0)).resolver(), downstream, 0);
        responseProducer.addDownstreamProducer(toDownstream);
        return responseProducer;
    }

    @Override
    protected ResponseProducer responseProducerReiterate(Request fromUpstream, ResponseProducer responseProducerPrevious,
                                                         int newIteration) {
        assert newIteration > responseProducerPrevious.iteration();
        LOG.debug("{}: Updating ResponseProducer for iteration '{}'", name(), newIteration);
        Plans.Plan plan = plans.getOrCreate(fromUpstream.partialAnswer().conceptMap().concepts().keySet(), resolvables, negateds);

        assert !plan.isEmpty();
        ResponseProducer responseProducerNewIter = responseProducerPrevious.newIteration(Iterators.empty(), newIteration);
        Mapped downstream = fromUpstream.partialAnswer()
                .mapToDownstream(Mapping.of(downstreamResolvers.get(plan.get(0)).mapping()));
        Request toDownstream = Request.create(self(), downstreamResolvers.get(plan.get(0)).resolver(), downstream, 0);
        responseProducerNewIter.addDownstreamProducer(toDownstream);
        return responseProducerNewIter;
    }

    class Plans {

        Map<Set<Retrievable>, Plan> plans;
        public Plans() { this.plans = new HashMap<>(); }

        public Plan getOrCreate(Set<Retrievable> boundVars, Set<Resolvable<?>> resolvable, Set<Negated> negations) {
            return plans.computeIfAbsent(boundVars, (bound) -> {
                List<Resolvable<?>> plan = planner.plan(resolvable, bound);
                plan.addAll(negations);
                return new Plan(plan);
            });
        }

        public Plan get(Set<Retrievable> boundVars) {
            assert plans.containsKey(boundVars);
            return plans.get(boundVars);
        }

        public class Plan {
            List<Resolvable<?>> plan;
            private Plan(List<Resolvable<?>> plan) { this.plan = plan; }

            public Resolvable<?> get(int index) {
                return plan.get(index);
            }

            public Resolvable<?> next(int index) {
                return plan.get(index + 1);
            }

            public boolean isLast(int index) {
                return index == plan.size() - 1;
            }

            public boolean isEmpty() { return plan.isEmpty(); }
        }
    }

    public static class Nested extends ConjunctionResolver<Nested> {

        public Nested(Actor<Nested> self, Conjunction conjunction, Actor<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry, TraversalEngine traversalEngine, ConceptManager conceptMgr, LogicManager logicMgr, Planner planner, boolean explanations) {
            super(self, Nested.class.getSimpleName() + "(pattern: " + conjunction + ")", conjunction, resolutionRecorder,
                  registry, traversalEngine, conceptMgr, logicMgr, planner, explanations);
        }

        @Override
        protected void nextAnswer(Request fromUpstream, ResponseProducer responseProducer, int iteration) {
            if (responseProducer.hasUpstreamAnswer()) {
                Partial<?> upstreamAnswer = responseProducer.upstreamAnswers().next();
                responseProducer.recordProduced(upstreamAnswer.conceptMap());
                answerToUpstream(upstreamAnswer, fromUpstream, iteration);
            } else {
                if (responseProducer.hasDownstreamProducer()) {
                    requestFromDownstream(responseProducer.nextDownstreamProducer(), fromUpstream, iteration);
                } else {
                    failToUpstream(fromUpstream, iteration);
                }
            }
        }

        @Override
        protected Optional<AnswerState> toUpstreamAnswer(Partial<?> fromDownstream) {
            return Optional.of(fromDownstream.asFiltered().toUpstream(self()));
        }

        @Override
        protected void exception(Throwable e) {
            LOG.error("Actor exception", e);
            // TODO, once integrated into the larger flow of executing queries, kill the resolvers and report and exception to root
        }
    }
}
