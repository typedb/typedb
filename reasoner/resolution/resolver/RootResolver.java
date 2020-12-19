/*
 * Copyright (C) 2020 Grakn Labs
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

import grakn.common.collection.Pair;
import grakn.common.concurrent.actor.Actor;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.logic.concludable.ConjunctionConcludable;
import grakn.core.pattern.Conjunction;
import grakn.core.reasoner.resolution.MockTransaction;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.MappingAggregator;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import grakn.core.traversal.TraversalEngine;
import graql.lang.pattern.variable.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.map;

/**
 * A root resolver is a special resolver: it is aware that it is not the child any resolver, so does not
 * pass any responses upwards. Instead, it can submit Answers or Exhausted statuses to the owner
 * of the Root resolver.
 */
public class RootResolver extends Resolver<RootResolver> {
    private static final Logger LOG = LoggerFactory.getLogger(RootResolver.class);

    private final Conjunction conjunction;
    private final Set<ConjunctionConcludable<?, ?>> conjunctionConcludables;
    private final Consumer<ResolutionAnswer> onAnswer;
    private final Consumer<Integer> onExhausted;
    private final List<Pair<Actor<ConcludableResolver>, Map<Reference.Name, Reference.Name>>> plannedConcludables;
    private final Actor<ResolutionRecorder> resolutionRecorder;
    private final TraversalEngine traversalEngine;
    private boolean isInitialised;
    private ResponseProducer responseProducer;

    public RootResolver(Actor<RootResolver> self, Conjunction conjunction, Consumer<ResolutionAnswer> onAnswer,
                        Consumer<Integer> onExhausted, Actor<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry,
                        TraversalEngine traversalEngine) {
        super(self, RootResolver.class.getSimpleName() + "(pattern:" + conjunction + ")", registry);
        this.conjunction = conjunction;
        this.onAnswer = onAnswer;
        this.onExhausted = onExhausted;
        this.resolutionRecorder = resolutionRecorder;
        this.traversalEngine = traversalEngine;
        this.isInitialised = false;
        this.conjunctionConcludables = ConjunctionConcludable.create(conjunction);
        this.plannedConcludables = new ArrayList<>();
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);
        if (!isInitialised) {
            initialiseDownstreamActors();
            isInitialised = true;
            responseProducer = responseProducerCreate(fromUpstream, iteration);
        }
        mayReiterateResponseProducer(fromUpstream, iteration);
        if (iteration < responseProducer.iteration()) {
            // short circuit if the request came from a prior iteration
            respondToUpstream(new Response.Exhausted(fromUpstream), iteration);
        } else {
            assert iteration == responseProducer.iteration();
            tryAnswer(fromUpstream, iteration);
        }
    }

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        LOG.trace("{}: received answer: {}", name(), fromDownstream);

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);

        ResolutionAnswer.Derivation derivation = fromDownstream.sourceRequest().partialResolutions();
        if (fromDownstream.answer().isInferred()) {
            derivation = derivation.withAnswer(fromDownstream.sourceRequest().receiver(), fromDownstream.answer());
        }

        ConceptMap conceptMap = fromDownstream.answer().aggregated().conceptMap();
        Actor<? extends Resolver<?>> sender = fromDownstream.sourceRequest().receiver();
        if (isLast(sender)) {
            if (!responseProducer.hasProduced(conceptMap)) {
                responseProducer.recordProduced(conceptMap);

                ResolutionAnswer answer = new ResolutionAnswer(fromUpstream.partialConceptMap().aggregateWith(conceptMap),
                                                               conjunction.toString(), derivation, self(),
                                                               fromDownstream.answer().isInferred());
                submitAnswer(answer);
            } else {
                tryAnswer(fromUpstream, iteration);
            }
        } else {
            Pair<Actor<ConcludableResolver>, Map<Reference.Name, Reference.Name>> nextPlannedDownstream = nextPlannedDownstream(sender);
            Request downstreamRequest = new Request(fromUpstream.path().append(nextPlannedDownstream.first()),
                                                    MappingAggregator.of(conceptMap, nextPlannedDownstream.second()), derivation);
            responseProducer.addDownstreamProducer(downstreamRequest);
            requestFromDownstream(downstreamRequest, fromUpstream, iteration);
        }
    }

    @Override
    protected void receiveExhausted(Response.Exhausted fromDownstream, int iteration) {
        LOG.trace("{}: received Exhausted: {}", name(), fromDownstream);
        responseProducer.removeDownstreamProducer(fromDownstream.sourceRequest());
        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        tryAnswer(fromUpstream, iteration);
    }

    @Override
    protected void initialiseDownstreamActors() {
        LOG.debug("{}: initialising downstream actors", name());

        // TODO Find the applicable rules for each, which requires 1 or more valid unifications with a rule.
        // TODO Mark concludables with no applicable rules as inconcludable
        // TODO Return to the conjunction now knowing the set of inconcludable constraints
        // TODO Tell the concludables to extend themselves by traversing all inconcludable constraints

        // Plan the order in which to execute the concludables
        List<ConjunctionConcludable<?, ?>> planned = list(conjunctionConcludables); // TODO Do some actual planning
        for (ConjunctionConcludable<?, ?> concludable : planned) {
            Pair<Actor<ConcludableResolver>, Map<Reference.Name, Reference.Name>> concludableUnifierPair = registry.registerConcludable(concludable); // TODO TraversalAnswerCount and Rules?
            plannedConcludables.add(concludableUnifierPair);
        }
    }

    @Override
    protected ResponseProducer responseProducerCreate(Request request, int iteration) {
        LOG.debug("{}: Creating a new ResponseProducer for request: {}", name(), request);
        Iterator<ConceptMap> traversal = (new MockTransaction(3L)).query(conjunction, new ConceptMap());
        ResponseProducer responseProducer = new ResponseProducer(traversal, iteration);
        Request toDownstream = new Request(request.path().append(plannedConcludables.get(0).first()),
                                           MappingAggregator.of(request.partialConceptMap().map(), plannedConcludables.get(0).second()),
                                           new ResolutionAnswer.Derivation(map()));
        responseProducer.addDownstreamProducer(toDownstream);

        return responseProducer;
    }

    @Override
    protected ResponseProducer responseProducerReiterate(Request request, ResponseProducer responseProducerPrevious, int newIteration) {
        assert newIteration > responseProducerPrevious.iteration();
        LOG.debug("{}: Updating ResponseProducer for iteration '{}'", name(), newIteration);

        assert newIteration > responseProducerPrevious.iteration();
        Iterator<ConceptMap> traversal = (new MockTransaction(3L)).query(conjunction, new ConceptMap());
        ResponseProducer responseProducerNewIter = responseProducerPrevious.newIteration(traversal, newIteration);
        Request toDownstream = new Request(request.path().append(plannedConcludables.get(0).first()),
                                           MappingAggregator.of(request.partialConceptMap().map(), plannedConcludables.get(0).second()),
                                           new ResolutionAnswer.Derivation(map()));
        responseProducerNewIter.addDownstreamProducer(toDownstream);
        return responseProducerNewIter;
    }

    @Override
    protected void exception(Exception e) {
        LOG.error("Actor exception", e);
        // TODO, once integrated into the larger flow of executing queries, kill the actors and report and exception to root
    }

    private void tryAnswer(Request fromUpstream, int iteration) {
        while (responseProducer.hasTraversalProducer()) {
            ConceptMap conceptMap = responseProducer.traversalProducer().next();
            LOG.trace("{}: has found via traversal: {}", name(), conceptMap);
            if (!responseProducer.hasProduced(conceptMap)) {
                responseProducer.recordProduced(conceptMap);
                ResolutionAnswer answer = new ResolutionAnswer(fromUpstream.partialConceptMap().aggregateWith(conceptMap),
                                                               conjunction.toString(), ResolutionAnswer.Derivation.EMPTY, self(), false);
                submitAnswer(answer);
            }
        }

        if (responseProducer.hasDownstreamProducer()) {
            requestFromDownstream(responseProducer.nextDownstreamProducer(), fromUpstream, iteration);
        } else {
            onExhausted.accept(iteration);
        }
    }

    private void mayReiterateResponseProducer(Request fromUpstream, int iteration) {
        if (responseProducer.iteration() + 1 == iteration) {
            responseProducer = responseProducerReiterate(fromUpstream, responseProducer, iteration);
        }
    }

    private void submitAnswer(ResolutionAnswer answer) {
        LOG.debug("Submitting root answer: {}", answer.aggregated());
        resolutionRecorder.tell(state -> state.record(answer));
        onAnswer.accept(answer);
    }

    private boolean isLast(Actor<? extends Resolver<?>> actor) {
        return plannedConcludables.get(plannedConcludables.size() - 1).first().equals(actor);
    }

    private Pair<Actor<ConcludableResolver>, Map<Reference.Name, Reference.Name>> nextPlannedDownstream(Actor<? extends Resolver<?>> actor) {
        int index = -1;
        for (int i = 0; i < plannedConcludables.size(); i++) {
            if (actor.equals(plannedConcludables.get(i).first())) { index = i; break; }
        }
        assert index != -1 && index < plannedConcludables.size() - 1;
        return plannedConcludables.get(index + 1);
    }
}
