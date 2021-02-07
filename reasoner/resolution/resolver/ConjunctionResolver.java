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

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.Iterators;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.logic.LogicManager;
import grakn.core.logic.resolvable.Concludable;
import grakn.core.logic.resolvable.Negated;
import grakn.core.logic.resolvable.Resolvable;
import grakn.core.logic.resolvable.Retrievable;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.pattern.Negation;
import grakn.core.pattern.variable.Variable;
import grakn.core.reasoner.resolution.Planner;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.answer.AnswerState.UpstreamVars.Initial;
import grakn.core.reasoner.resolution.answer.Mapping;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier;
import graql.lang.pattern.variable.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static grakn.common.collection.Collections.map;
import static grakn.core.common.exception.ErrorMessage.Pattern.UNSATISFIABLE_CONJUNCTION;
import static grakn.core.common.iterator.Iterators.iterate;

public abstract class ConjunctionResolver<T extends ConjunctionResolver<T>> extends Resolver<T> {

    private static final Logger LOG = LoggerFactory.getLogger(Root.Conjunction.class);

    private final LogicManager logicMgr;
    private final Planner planner;
    final ConceptManager conceptMgr;
    final grakn.core.pattern.Conjunction conjunction;
    final Actor<ResolutionRecorder> resolutionRecorder;
    final List<Resolvable<?>> plan;
    final Map<Request, ResponseProducer> responseProducers;
    final Map<Resolvable<?>, ResolverRegistry.MappedResolver> downstreamResolvers;
    private boolean isInitialised;

    public ConjunctionResolver(Actor<T> self, String name, grakn.core.pattern.Conjunction conjunction,
                               Actor<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry,
                               TraversalEngine traversalEngine, ConceptManager conceptMgr, LogicManager logicMgr,
                               Planner planner, boolean explanations) {
        super(self, name, registry, traversalEngine, explanations);
        this.conceptMgr = conceptMgr;
        this.logicMgr = logicMgr;
        this.resolutionRecorder = resolutionRecorder;
        this.planner = planner;
        this.conjunction = conjunction;
        this.plan = new ArrayList<>();
        this.responseProducers = new HashMap<>();
        this.isInitialised = false;
        this.downstreamResolvers = new HashMap<>();
    }

    protected abstract void tryAnswer(Request fromUpstream, ResponseProducer responseProducer, int iteration);

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);

        if (!isInitialised) {
            initialiseDownstreamActors();
            isInitialised = true;
        }

        ResponseProducer responseProducer = mayUpdateAndGetResponseProducer(fromUpstream, iteration);
        if (iteration < responseProducer.iteration()) {
            // short circuit if the request came from a prior iteration
            respondToUpstream(new Response.Fail(fromUpstream), iteration);
        } else {
            assert iteration == responseProducer.iteration();
            tryAnswer(fromUpstream, responseProducer, iteration);
        }
    }

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        LOG.trace("{}: received Answer: {}", name(), fromDownstream);

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);

        ResolutionAnswer.Derivation derivation;
        if (explanations()) {
            // TODO revisit
            derivation = fromDownstream.sourceRequest().partialResolutions();
            if (fromDownstream.answer().isInferred()) {
                derivation = derivation.withAnswer(fromDownstream.sourceRequest().receiver(), fromDownstream.answer());
            }
        } else {
            derivation = null;
        }


        // TODO this is a hack, we want requests to a negation to be "single use", otherwise we can end up in an infinite loop, where
        // The request to the negation never gets removed and we constantly re-request from it!
        // TODO this could be either implemented with a different response type: FinalAnswer
        // TODO -> we don't need to use Fail as often, and only when there's no first answer at all
        // TODO alternatively, we could create a ReusableRequest and SingleUseRequest object
        if (toDownstream.receiver().state instanceof NegationResolver) responseProducer.removeDownstreamProducer(toDownstream);

        ConceptMap conceptMap = fromDownstream.answer().derived().withInitialFiltered();
        if (fromDownstream.planIndex() == plan.size() - 1) {
            Optional<AnswerState.UpstreamVars.Derived> answer = toUpstreamAnswer(fromUpstream, conceptMap);
            if (answer.isPresent() && !responseProducer.hasProduced(answer.get().withInitialFiltered())) {
                responseProducer.recordProduced(answer.get().withInitialFiltered());
                ResolutionAnswer resolutionAnswer = new ResolutionAnswer(answer.get(), conjunction.toString(), derivation, self(),
                                                                         fromDownstream.answer().isInferred());
                respondToUpstream(Response.Answer.create(fromUpstream, resolutionAnswer), iteration);
            } else {
                tryAnswer(fromUpstream, responseProducer, iteration);
            }
        } else {
            int planIndex = fromDownstream.planIndex() + 1;
            ResolverRegistry.MappedResolver nextPlannedDownstream = downstreamResolvers.get(plan.get(planIndex));
            Request downstreamRequest = Request.create(fromUpstream.path().append(nextPlannedDownstream.resolver()),
                                                       Initial.of(conceptMap).toDownstreamVars(
                                                               Mapping.of(nextPlannedDownstream.mapping())),
                                                       derivation, planIndex, null);
            responseProducer.addDownstreamProducer(downstreamRequest);
            requestFromDownstream(downstreamRequest, fromUpstream, iteration);
        }
    }

    @Override
    protected void receiveExhausted(Response.Fail fromDownstream, int iteration) {
        LOG.trace("{}: Receiving Exhausted: {}", name(), fromDownstream);
        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);

        if (iteration < responseProducer.iteration()) {
            // short circuit old iteration exhausted messages to upstream
            respondToUpstream(new Response.Fail(fromUpstream), iteration);
            return;
        }

        responseProducer.removeDownstreamProducer(fromDownstream.sourceRequest());
        tryAnswer(fromUpstream, responseProducer, iteration);
    }

    @Override
    protected void initialiseDownstreamActors() {
        LOG.debug("{}: initialising downstream actors", name());
        Set<Concludable> concludables = Iterators.iterate(Concludable.create(conjunction))
                .filter(c -> c.getApplicableRules(conceptMgr, logicMgr).hasNext()).toSet();
        Set<Retrievable> retrievables = Retrievable.extractFrom(conjunction, concludables);
        Set<Resolvable<?>> resolvables = new HashSet<>();
        resolvables.addAll(concludables);
        resolvables.addAll(retrievables);

        plan.addAll(planner.plan(resolvables));
        iterate(plan).forEachRemaining(resolvable -> {
            downstreamResolvers.put(resolvable, registry.registerResolvable(resolvable));
        });

        // TODO just adding negations at the end, but we will want to include them in the planner
        for (Negation negation : conjunction.negations()) {
            Disjunction disjunction = negation.disjunction();
            Set<Reference.Name> filter = iterate(conjunction.variables()).filter(v -> v.reference().isName())
                    .map(v -> v.reference().asName()).toSet();
            Disjunction satisfiableDisjunction = resolveTypesAndFilter(disjunction, filter);
            if (satisfiableDisjunction.conjunctions().isEmpty()) return;

            Negated negated = new Negated(satisfiableDisjunction);
            plan.add(negated);
            downstreamResolvers.put(negated, registry.negated(conjunction, negated));
        }
    }

    // TODO figure out how not to duplicate these
    private Disjunction resolveTypesAndFilter(Disjunction disjunction, Set<Reference.Name> filter) {
        disjunction.conjunctions().forEach(conj -> logicMgr.typeResolver().resolve(conj));
        for (Conjunction conjunction : disjunction.conjunctions()) {
            if (!conjunction.isSatisfiable() && !conjunction.isBounded() && conjunctionContainsThings(conjunction, filter)) {
                // TODO this should kill the actors?
                throw GraknException.of(UNSATISFIABLE_CONJUNCTION, conjunction);
            }
        }
        List<Conjunction> satisfiable = iterate(disjunction.conjunctions()).filter(Conjunction::isSatisfiable).toList();
        return new Disjunction(satisfiable);
    }

    private boolean conjunctionContainsThings(Conjunction conjunction, Set<Reference.Name> filter) {
        return !filter.isEmpty() && iterate(filter).anyMatch(id -> conjunction.variable(Identifier.Variable.of(id)).isThing()) ||
                iterate(conjunction.variables()).anyMatch(Variable::isThing);
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

//        ResourceIterator<AnswerState.UpstreamVars.Derived> upstreamAnswers = toUpstreamAnswers(
//                fromUpstream, compatibleBoundAnswers(conceptMgr, conjunction, fromUpstream.partialAnswer().conceptMap()));
//
        ResponseProducer responseProducer = new ResponseProducer(Iterators.empty(), iteration);
        assert !plan.isEmpty();
        Request toDownstream = Request.create(fromUpstream.path().append(downstreamResolvers.get(plan.get(0)).resolver()),
                                              Initial.of(fromUpstream.partialAnswer().conceptMap())
                                                      .toDownstreamVars(Mapping.of(downstreamResolvers.get(plan.get(0)).mapping())),
                                              new ResolutionAnswer.Derivation(map()), 0, null);
        responseProducer.addDownstreamProducer(toDownstream);
        return responseProducer;
    }

    @Override
    protected ResponseProducer responseProducerReiterate(Request fromUpstream, ResponseProducer responseProducerPrevious,
                                                         int newIteration) {
        assert newIteration > responseProducerPrevious.iteration();
        LOG.debug("{}: Updating ResponseProducer for iteration '{}'", name(), newIteration);

//        ResourceIterator<AnswerState.UpstreamVars.Derived> upstreamAnswers = toUpstreamAnswers(
//                fromUpstream, compatibleBoundAnswers(conceptMgr, conjunction, fromUpstream.partialAnswer().conceptMap()));

        ResponseProducer responseProducerNewIter = responseProducerPrevious.newIteration(Iterators.empty(), newIteration);
        assert !plan.isEmpty();
        Request toDownstream = Request.create(fromUpstream.path().append(downstreamResolvers.get(plan.get(0)).resolver()),
                                              Initial.of(fromUpstream.partialAnswer().conceptMap())
                                                      .toDownstreamVars(Mapping.of(downstreamResolvers.get(plan.get(0)).mapping())),
                                              new ResolutionAnswer.Derivation(map()), 0, null);
        responseProducerNewIter.addDownstreamProducer(toDownstream);
        return responseProducerNewIter;
    }

    protected abstract ResourceIterator<AnswerState.UpstreamVars.Derived> toUpstreamAnswers(Request fromUpstream,
                                                                                            ResourceIterator<ConceptMap> downstreamConceptMaps);

    protected abstract Optional<AnswerState.UpstreamVars.Derived> toUpstreamAnswer(Request fromUpstream, ConceptMap downstreamConceptMap);

    public static class Simple extends ConjunctionResolver<Simple> {

        public Simple(Actor<Simple> self, Conjunction conjunction, Actor<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry, TraversalEngine traversalEngine, ConceptManager conceptMgr, LogicManager logicMgr, Planner planner, boolean explanations) {
            super(self, Simple.class.getSimpleName() + "(pattern: " + conjunction + ")", conjunction, resolutionRecorder,
                  registry, traversalEngine, conceptMgr, logicMgr, planner, explanations);
        }

        @Override
        protected void tryAnswer(Request fromUpstream, ResponseProducer responseProducer, int iteration) {
            if (responseProducer.hasUpstreamAnswer()) {
                AnswerState.UpstreamVars.Derived upstreamAnswer = responseProducer.upstreamAnswers().next();
                responseProducer.recordProduced(upstreamAnswer.withInitialFiltered());
                ResolutionAnswer answer = new ResolutionAnswer(upstreamAnswer, conjunction.toString(),
                                                               ResolutionAnswer.Derivation.EMPTY, self(), false);
                respondToUpstream(Response.Answer.create(fromUpstream, answer), iteration);
            } else {
                if (responseProducer.hasDownstreamProducer()) {
                    requestFromDownstream(responseProducer.nextDownstreamProducer(), fromUpstream, iteration);
                } else {
                    respondToUpstream(new Response.Fail(fromUpstream), iteration);
                }
            }
        }

        @Override
        protected ResourceIterator<AnswerState.UpstreamVars.Derived> toUpstreamAnswers(Request fromUpstream,
                                                                                       ResourceIterator<ConceptMap> downstreamConceptMaps) {
            assert !fromUpstream.filter().isPresent();
            return downstreamConceptMaps.map(conceptMap -> fromUpstream.partialAnswer().asIdentity()
                    .aggregateToUpstream(conceptMap, null));
        }

        @Override
        protected Optional<AnswerState.UpstreamVars.Derived> toUpstreamAnswer(Request fromUpstream, ConceptMap downstreamConceptMap) {
            assert !fromUpstream.filter().isPresent();
            return Optional.of(fromUpstream.partialAnswer().asIdentity().aggregateToUpstream(downstreamConceptMap, null));
        }

        @Override
        protected void exception(Throwable e) {
            LOG.error("Actor exception", e);
            // TODO, once integrated into the larger flow of executing queries, kill the actors and report and exception to root
        }
    }
}
