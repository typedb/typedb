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

import grakn.core.common.exception.GraknCheckedException;
import grakn.core.common.exception.GraknException;
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
import grakn.core.reasoner.resolution.answer.Mapping;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.Response;
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

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.iterator.Iterators.iterate;

public abstract class ConjunctionResolver<T extends ConjunctionResolver<T, R>, R extends CompoundResolver.Responses>
        extends CompoundResolver<T, R> {

    private static final Logger LOG = LoggerFactory.getLogger(ConjunctionResolver.class);

    private final LogicManager logicMgr;
    private final Planner planner;
    final grakn.core.pattern.Conjunction conjunction;
    final Set<Resolvable<?>> resolvables;
    final Set<Negated> negateds;
    final Plans plans;
    final Map<Resolvable<?>, ResolverRegistry.ResolverView> downstreamResolvers;

    public ConjunctionResolver(Actor<T> self, String name, grakn.core.pattern.Conjunction conjunction,
                               Actor<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry,
                               TraversalEngine traversalEngine, ConceptManager conceptMgr, LogicManager logicMgr,
                               Planner planner, boolean resolutionTracing) {
        super(self, name, registry, traversalEngine, conceptMgr, resolutionTracing, resolutionRecorder);
        this.logicMgr = logicMgr;
        this.planner = planner;
        this.conjunction = conjunction;
        this.resolvables = new HashSet<>();
        this.negateds = new HashSet<>();
        this.plans = new Plans();
        this.downstreamResolvers = new HashMap<>();
    }

    protected abstract void nextAnswer(Request fromUpstream, R responseProducer, int iteration);

    abstract Optional<AnswerState> toUpstreamAnswer(Partial<?> fromDownstream);

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        LOG.trace("{}: received Answer: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        R responses = this.responses.get(fromUpstream);

        Plans.Plan plan = plans.get(fromUpstream.partialAnswer().conceptMap().concepts().keySet());

        // TODO: this is a bit of a hack, we want requests to a negation to be "single use", otherwise we can end up in an infinite loop
        // TODO: where the request to the negation never gets removed and we constantly re-request from it!
        // TODO: this could be either implemented with a different response type: FinalAnswer, or splitting Request into ReusableRequest vs SingleRequest
        if (plan.get(toDownstream.planIndex()).isNegated()) responses.removeDownstreamProducer(toDownstream);

        if (plan.isLast(fromDownstream.planIndex())) {
            Optional<AnswerState> upstreamAnswer = toUpstreamAnswer(fromDownstream.answer());
            boolean answerAccepted = upstreamAnswer.isPresent() && tryAcceptUpstreamAnswer(upstreamAnswer.get(), fromUpstream, iteration);
            if (!answerAccepted) nextAnswer(fromUpstream, responses, iteration);
        } else {
            int nextResolverIndex = fromDownstream.planIndex() + 1;
            ResolverRegistry.ResolverView nextPlannedDownstream = downstreamResolvers.get(plan.get(nextResolverIndex));
            Partial<?> downstream = forDownstreamResolver(nextPlannedDownstream, fromDownstream.answer());
            Request downstreamRequest = Request.create(self(), nextPlannedDownstream.resolver(), downstream, nextResolverIndex);
            responses.addDownstreamProducer(downstreamRequest);
            requestFromDownstream(downstreamRequest, fromUpstream, iteration);
        }
    }

    abstract boolean tryAcceptUpstreamAnswer(AnswerState upstreamAnswer, Request fromUpstream, int iteration);

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        LOG.trace("{}: Receiving Exhausted: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        R responses = this.responses.get(fromUpstream);

        if (iteration < responses.iteration()) {
            // short circuit old iteration failed messages to upstream
            failToUpstream(fromUpstream, iteration);
            return;
        }

        responses.removeDownstreamProducer(fromDownstream.sourceRequest());
        nextAnswer(fromUpstream, responses, iteration);
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        LOG.debug("{}: initialising downstream resolvers", name());
        Set<Concludable> concludables = Iterators.iterate(Concludable.create(conjunction))
                .filter(c -> c.getApplicableRules(conceptMgr, logicMgr).hasNext()).toSet();
        Set<grakn.core.logic.resolvable.Retrievable> retrievables = grakn.core.logic.resolvable.Retrievable.extractFrom(conjunction, concludables);
        resolvables.addAll(concludables);
        resolvables.addAll(retrievables);
        iterate(resolvables).forEachRemaining(resolvable -> {
            try {
                downstreamResolvers.put(resolvable, registry.registerResolvable(resolvable));
            } catch (GraknCheckedException e) {
                terminate(e);
            }
        });
        for (Negation negation : conjunction.negations()) {
            Negated negated = new Negated(negation);
            try {
                downstreamResolvers.put(negated, registry.negated(negated, conjunction));
                negateds.add(negated);
            } catch (GraknCheckedException e) {
                terminate(e);
            }
        }
        if (!isTerminated()) isInitialised = true;
    }

    @Override
    protected R responsesCreate(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating a new Responses for request: {}", name(), fromUpstream);
        Plans.Plan plan = plans.getOrCreate(fromUpstream.partialAnswer().conceptMap().concepts().keySet(), resolvables, negateds);
        assert !plan.isEmpty();

        R responses = responsesNew(iteration);
        ResolverRegistry.ResolverView childResolver = downstreamResolvers.get(plan.get(0));
        Partial<?> downstream = forDownstreamResolver(childResolver, fromUpstream.partialAnswer());
        Request toDownstream = Request.create(self(), childResolver.resolver(), downstream, 0);
        responses.addDownstreamProducer(toDownstream);
        return responses;
    }

    @Override
    protected R responsesReiterate(Request fromUpstream, R responsesPrior,
                                   int newIteration) {
        assert newIteration > responsesPrior.iteration();
        LOG.debug("{}: Updating Responses for iteration '{}'", name(), newIteration);
        Plans.Plan plan = plans.getOrCreate(fromUpstream.partialAnswer().conceptMap().concepts().keySet(), resolvables, negateds);

        assert !plan.isEmpty();
        R responsesNextIteration = responsesForIteration(responsesPrior, newIteration);
        ResolverRegistry.ResolverView childResolver = downstreamResolvers.get(plan.get(0));
        Partial<?> downstream = forDownstreamResolver(childResolver, fromUpstream.partialAnswer());
        Request toDownstream = Request.create(self(), childResolver.resolver(), downstream, 0);
        responsesNextIteration.addDownstreamProducer(toDownstream);
        return responsesNextIteration;
    }

    abstract R responsesNew(int iteration);

    abstract R responsesForIteration(R responsesPrior, int iteration);

    Partial<?> forDownstreamResolver(ResolverRegistry.ResolverView resolver, Partial<?> partialAnswer) {
        if (resolver.isMapped()) {
            return partialAnswer
                    .mapToDownstream(Mapping.of(resolver.asMapped().mapping()), resolver.resolver());
        } else if (resolver.isFiltered()) {
            return partialAnswer
                    .filterToDownstream(resolver.asFiltered().filter(), resolver.resolver());
        } else {
            throw GraknException.of(ILLEGAL_STATE);
        }
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

    public static class Nested extends ConjunctionResolver<Nested, CompoundResolver.Responses> {

        public Nested(Actor<Nested> self, Conjunction conjunction, Actor<ResolutionRecorder> resolutionRecorder,
                      ResolverRegistry registry, TraversalEngine traversalEngine, ConceptManager conceptMgr,
                      LogicManager logicMgr, Planner planner, boolean resolutionTracing) {
            super(self, Nested.class.getSimpleName() + "(pattern: " + conjunction + ")", conjunction,
                  resolutionRecorder, registry, traversalEngine, conceptMgr, logicMgr, planner, resolutionTracing);
        }

        @Override
        protected void nextAnswer(Request fromUpstream, CompoundResolver.Responses responses, int iteration) {
            if (responses.hasDownstreamProducer()) {
                requestFromDownstream(responses.nextDownstreamProducer(), fromUpstream, iteration);
            } else {
                failToUpstream(fromUpstream, iteration);
            }
        }

        @Override
        protected Optional<AnswerState> toUpstreamAnswer(Partial<?> fromDownstream) {
            return Optional.of(fromDownstream.asFiltered().toUpstream());
        }

        @Override
        boolean tryAcceptUpstreamAnswer(AnswerState upstreamAnswer, Request fromUpstream, int iteration) {
            answerToUpstream(upstreamAnswer, fromUpstream, iteration);
            return true;
        }

        @Override
        CompoundResolver.Responses responsesNew(int iteration) {
            return new CompoundResolver.Responses(iteration);
        }

        @Override
        CompoundResolver.Responses responsesForIteration(CompoundResolver.Responses responsesPrior, int iteration) {
            return new CompoundResolver.Responses(iteration);
        }
    }
}
