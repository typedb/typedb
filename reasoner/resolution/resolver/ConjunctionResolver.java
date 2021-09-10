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
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Negated;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Negation;
import com.vaticle.typedb.core.reasoner.resolution.Planner;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial;
import com.vaticle.typedb.core.reasoner.resolution.answer.Mapping;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer.Traced;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer.Traced.trace;

public abstract class ConjunctionResolver<RESOLVER extends ConjunctionResolver<RESOLVER>> extends CompoundResolver<RESOLVER> {

    private static final Logger LOG = LoggerFactory.getLogger(ConjunctionResolver.class);

    final Set<Resolvable<?>> resolvables;
    final Set<Negated> negateds;
    final Plans plans;
    final Map<Resolvable<?>, ResolverRegistry.ResolverView> downstreamResolvers;

    protected ConjunctionResolver(Driver<RESOLVER> driver, String name, ResolverRegistry registry) {
        super(driver, name, registry);
        this.resolvables = new HashSet<>();
        this.negateds = new HashSet<>();
        this.plans = new Plans();
        this.downstreamResolvers = new HashMap<>();
    }

    abstract Set<Concludable> concludablesTriggeringRules();

    abstract Conjunction conjunction();

    abstract Optional<AnswerState> toUpstreamAnswer(Partial.Compound<?, ?> fromDownstream);

    @Override
    protected void receiveAnswer(Traced<Response.Answer> fromDownstream) {
        LOG.trace("{}: received Answer: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request.Factory toDownstream = fromDownstream.message().sourceRequest();
        Traced<Request> fromUpstream = upstreamTracedRequest(fromDownstream);
        Request.Factory factory = fromUpstream.message().visit().factory();
        RequestState requestState = requestStates.get(factory);

        Plans.Plan plan = plans.getActive(factory);

        // TODO: this is a bit of a hack, we want requests to a negation to be "single use", otherwise we can end up in an infinite loop
        //  where the request to the negation never gets removed and we constantly re-request from it!
        //  this could be either implemented with a different response type: FinalAnswer, or splitting Visit into ReusableRequest vs SingleRequest
        if (plan.get(toDownstream.planIndex()).isNegated()) requestState.downstreamManager().remove(toDownstream);

        Partial.Compound<?, ?> partialAnswer = fromDownstream.message().answer().asCompound();
        if (plan.isLast(fromDownstream.message().planIndex())) {
            Optional<AnswerState> upstreamAnswer = toUpstreamAnswer(partialAnswer);
            boolean answerAccepted = upstreamAnswer.isPresent() && tryAcceptUpstreamAnswer(upstreamAnswer.get(), fromUpstream);
            if (!answerAccepted) nextAnswer(trace(factory.createVisit(fromDownstream.trace()), fromDownstream.trace()), requestState);
        } else {
            toNextChild(fromDownstream, factory, requestState, plan);
        }
    }

    boolean tryAcceptUpstreamAnswer(AnswerState upstreamAnswer, Traced<Request> fromUpstream) {
        RequestState requestState = requestStates.get(fromUpstream.message().visit().factory());
        if (!requestState.deduplicationSet().contains(upstreamAnswer.conceptMap())) {
            requestState.deduplicationSet().add(upstreamAnswer.conceptMap());
            answerToUpstream(upstreamAnswer, fromUpstream);
            return true;
        } else {
            return false;
        }
    }

    private void toNextChild(Traced<Response.Answer> fromDownstream, Request.Factory fromUpstream,
                             RequestState requestState, Plans.Plan plan) {
        int nextResolverIndex = fromDownstream.message().planIndex() + 1;
        Resolvable<?> nextResolvable = plan.get(nextResolverIndex);
        ResolverRegistry.ResolverView nextPlannedDownstream = downstreamResolvers.get(nextResolvable);
        final Partial<?> downstreamAns = toDownstream(fromDownstream.message().answer().asCompound(), nextPlannedDownstream, nextResolvable);
        Request.Factory downstream = Request.Factory.create(driver(), nextPlannedDownstream.resolver(), downstreamAns, nextResolverIndex);
        visitDownstream(downstream, trace(fromUpstream.createVisit(fromDownstream.trace()), fromDownstream.trace()));
        // negated requests can be used twice in a parallel setting, and return the same answer twice
        if (!nextResolvable.isNegated() || (nextResolvable.isNegated() && !requestState.downstreamManager().contains(downstream))) {
            requestState.downstreamManager().add(downstream);
        }
    }

    @Override
    protected void receiveFail(Traced<Response.Fail> fromDownstream) {
        LOG.trace("{}: Receiving Exhausted: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request.Factory downstream = fromDownstream.message().sourceRequest();
        Traced<Request> fromUpstream = upstreamTracedRequest(fromDownstream);
        RequestState requestState = this.requestStates.get(fromUpstream.message().visit().factory());
        requestState.downstreamManager().remove(downstream);
        nextAnswer(fromUpstream, requestState);
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        LOG.debug("{}: initialising downstream resolvers", name());
        Set<Concludable> concludables = concludablesTriggeringRules();
        Set<Retrievable> retrievables = Retrievable.extractFrom(conjunction(), concludables);
        resolvables.addAll(concludables);
        resolvables.addAll(retrievables);
        iterate(resolvables).forEachRemaining(resolvable -> {
            try {
                downstreamResolvers.put(resolvable, registry.registerResolvable(resolvable));
            } catch (TypeDBException e) {
                terminate(e);
            }
        });
        for (Negation negation : conjunction().negations()) {
            Negated negated = new Negated(negation);
            try {
                downstreamResolvers.put(negated, registry.negated(negated, conjunction()));
                negateds.add(negated);
            } catch (TypeDBException e) {
                terminate(e);
            }
        }
        if (!isTerminated()) isInitialised = true;
    }

    @Override
    protected RequestState requestStateCreate(Request.Factory fromUpstream) {
        LOG.debug("{}: Creating a new RequestState for request: {}", name(), fromUpstream);
        Plans.Plan plan = plans.create(fromUpstream, resolvables, negateds);
        assert !plan.isEmpty() && fromUpstream.partialAnswer().isCompound();
        RequestState requestState = requestStateNew();
        initialiseRequestState(requestState, fromUpstream, plan);
        return requestState;
    }

    private void initialiseRequestState(RequestState requestState, Request.Factory fromUpstream, Plans.Plan plan) {
        ResolverRegistry.ResolverView childResolver = downstreamResolvers.get(plan.get(0));
        Partial<?> downstream = toDownstream(fromUpstream.partialAnswer().asCompound(), childResolver, plan.get(0));
        requestState.downstreamManager().add(Request.Factory.create(driver(), childResolver.resolver(), downstream, 0));
    }

    private Partial<?> toDownstream(Partial.Compound<?, ?> partialAnswer, ResolverRegistry.ResolverView nextDownstream,
                                    Resolvable<?> nextResolvable) {
        assert downstreamResolvers.get(nextResolvable).equals(nextDownstream);
        if (nextDownstream.isMappedConcludable()) {
            return partialAnswer.toDownstream(Mapping.of(nextDownstream.asMappedConcludable().mapping()),
                                              nextResolvable.asConcludable());
        } else if (nextDownstream.isFilteredNegation()) {
            return partialAnswer.filterToNestable(nextDownstream.asFilteredNegation().filter());
        } else if (nextDownstream.isFilteredRetrievable()) {
            return partialAnswer.filterToRetrievable(nextDownstream.asFilteredRetrievable().filter());
        } else {
            throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    abstract RequestState requestStateNew();

    static class Plans {
        private final Map<ConceptMap, Plan> plans;
        private final Map<Request.Factory, Plan> activePlans;
        private final Map<Resolvable<?>, Map<ConceptMap, Integer>> visitedWithBoundsCount;

        public Plans() {
            this.plans = new HashMap<>();
            this.activePlans = new HashMap<>();
            this.visitedWithBoundsCount = new HashMap<>();
        }

        public Plan create(Request.Factory fromUpstream, Set<Resolvable<?>> resolvables, Set<Negated> negations) {
            ConceptMap bounds = fromUpstream.partialAnswer().conceptMap();
            updateCountsWithBounds(resolvables, bounds);
            Map<Resolvable<?>, Integer> visitCounts = getCountsForBounds(resolvables, bounds);
            Plan plan = plans.computeIfAbsent(bounds, (ignored) -> {
                List<Resolvable<?>> newPlan = Planner.plan(resolvables, visitCounts, bounds.concepts().keySet());
                newPlan.addAll(negations);
                return new Plan(newPlan);
            });
            activePlans.put(fromUpstream, plan);
            return plan;
        }

        public Plan getActive(Request.Factory fromUpstream) {
            assert activePlans.containsKey(fromUpstream);
            return activePlans.get(fromUpstream);
        }

        private void updateCountsWithBounds(Set<Resolvable<?>> resolvables, ConceptMap bounds) {
            for (Resolvable<?> resolvable : resolvables) {
                visitedWithBoundsCount.putIfAbsent(resolvable, new HashMap<>());
                Map<ConceptMap, Integer> countsForBounds = visitedWithBoundsCount.get(resolvable);
                ConceptMap filtered = bounds.filter(resolvable.retrieves());
                countsForBounds.putIfAbsent(filtered, 0);
                int newCount = countsForBounds.get(filtered) + 1;
                countsForBounds.put(filtered, newCount);
            }
        }

        private Map<Resolvable<?>, Integer> getCountsForBounds(Set<Resolvable<?>> resolvables, ConceptMap bounds) {
            Map<Resolvable<?>, Integer> visitCounts = new HashMap<>();
            for (Resolvable<?> resolvable : resolvables) {
                Map<ConceptMap, Integer> counts = visitedWithBoundsCount.get(resolvable);
                visitCounts.put(resolvable, counts.getOrDefault(bounds, 0));
            }
            return visitCounts;
        }

        public static class Plan {

            final List<Resolvable<?>> plan;

            private Plan(List<Resolvable<?>> plan) { this.plan = plan; }

            public Resolvable<?> get(int index) {
                return plan.get(index);
            }

            public boolean isLast(int index) {
                return index == plan.size() - 1;
            }

            public boolean isEmpty() { return plan.isEmpty(); }
        }
    }

    public static class Nested extends ConjunctionResolver<Nested> {

        private final Conjunction conjunction;

        public Nested(Driver<Nested> driver, Conjunction conjunction, ResolverRegistry registry) {
            super(driver, Nested.class.getSimpleName() + "(pattern: " + conjunction + ")", registry);
            this.conjunction = conjunction;
        }

        @Override
        Set<Concludable> concludablesTriggeringRules() {
            return Iterators.iterate(Concludable.create(conjunction))
                    .filter(c -> c.getApplicableRules(registry.conceptManager(), registry.logicManager()).hasNext())
                    .toSet();
        }

        @Override
        Conjunction conjunction() {
            return conjunction;
        }

        @Override
        protected Optional<AnswerState> toUpstreamAnswer(Partial.Compound<?, ?> partialAnswer) {
            return Optional.of(partialAnswer.asNestable().toUpstream());
        }

        @Override
        RequestState requestStateNew() {
            return new RequestState();
        }

    }
}
