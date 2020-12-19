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

import grakn.common.collection.Either;
import grakn.common.collection.Pair;
import grakn.common.concurrent.actor.Actor;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.logic.Rule;
import grakn.core.logic.concludable.ConjunctionConcludable;
import grakn.core.pattern.Conjunction;
import grakn.core.reasoner.resolution.MockTransaction;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.MappingAggregator;
import grakn.core.reasoner.resolution.framework.ChildResolver;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import graql.lang.pattern.variable.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.map;

// TODO unify and materialise in receiveAnswer
public class RuleResolver extends ChildResolver<RuleResolver> {
    private static final Logger LOG = LoggerFactory.getLogger(RuleResolver.class);

    final Conjunction conjunction;
    private final Set<ConjunctionConcludable<?, ?>> conjunctionConcludables;
    private final List<Pair<Actor<ConcludableResolver>, Map<Reference.Name, Reference.Name>>> plannedConcludables;

    public RuleResolver(Actor<RuleResolver> self, Rule rule) {
        super(self, RuleResolver.class.getSimpleName() + "(rule:" + rule + ")");
        this.conjunction = rule.when();
        this.conjunctionConcludables = rule.whenConcludables();
        this.plannedConcludables = new ArrayList<>();
    }

    @Override
    protected void initialiseDownstreamActors(ResolverRegistry registry) {

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
        Iterator<ConceptMap> traversal = (new MockTransaction(3L)).query(conjunction, new ConceptMap());
        ResponseProducer responseProducer = new ResponseProducer(traversal, iteration);
        Request toDownstream = new Request(request.path().append(plannedConcludables.get(0).first()),
                                           MappingAggregator.of(request.partialConceptMap().map(), plannedConcludables.get(0).second()),
                                           new ResolutionAnswer.Derivation(map()));
        responseProducer.addDownstreamProducer(toDownstream);

        return responseProducer;
    }

    @Override
    protected ResponseProducer responseProducerReiterate(Request request, ResponseProducer responseProducer, int newIteration) {
        Iterator<ConceptMap> traversal = (new MockTransaction(3L)).query(conjunction, new ConceptMap());
        ResponseProducer responseProducerNewIter = responseProducer.newIteration(traversal, newIteration);
        Request toDownstream = new Request(request.path().append(plannedConcludables.get(0).first()),
                                           MappingAggregator.of(request.partialConceptMap().map(), plannedConcludables.get(0).second()),
                                           new ResolutionAnswer.Derivation(map()));
        responseProducerNewIter.addDownstreamProducer(toDownstream);
        return responseProducerNewIter;
    }

    @Override
    public Either<Request, Response> receiveRequest(Request fromUpstream, ResponseProducer responseProducer) {
        return messageToSend(fromUpstream, responseProducer);
    }

    @Override
    public Either<Request, Response> receiveExhausted(Request fromUpstream, Response.Exhausted fromDownstream, ResponseProducer responseProducer) {
        responseProducer.removeDownstreamProducer(fromDownstream.sourceRequest());
        return messageToSend(fromUpstream, responseProducer);
    }

    @Override
    public Either<Request, Response> receiveAnswer(Request fromUpstream, Response.Answer fromDownstream, ResponseProducer responseProducer) {
        Actor<? extends Resolver<?>> sender = fromDownstream.sourceRequest().receiver();
        ConceptMap conceptMap = fromDownstream.answer().aggregated().conceptMap();

        ResolutionAnswer.Derivation derivation = fromDownstream.sourceRequest().partialResolutions();
        if (fromDownstream.answer().isInferred()) {
            derivation = derivation.withAnswer(fromDownstream.sourceRequest().receiver(), fromDownstream.answer());
        }

        if (isLast(sender)) {
            LOG.trace("{}: has produced: {}", name, conceptMap);

            if (!responseProducer.hasProduced(conceptMap)) {
                responseProducer.recordProduced(conceptMap);

                ResolutionAnswer answer = new ResolutionAnswer(fromUpstream.partialConceptMap().aggregateWith(conceptMap),
                                                               conjunction.toString(), derivation, self(), true);
                return Either.second(new Response.Answer(fromUpstream, answer));
            } else {
                return messageToSend(fromUpstream, responseProducer);
            }
        } else {
            Pair<Actor<ConcludableResolver>, Map<Reference.Name, Reference.Name>> nextPlannedDownstream = nextPlannedDownstream(sender);
            Request downstreamRequest = new Request(fromUpstream.path().append(nextPlannedDownstream.first()),
                                                    MappingAggregator.of(conceptMap, nextPlannedDownstream.second()), derivation);
            responseProducer.addDownstreamProducer(downstreamRequest);
            return Either.first(downstreamRequest);
        }
    }

    @Override
    protected void exception(Exception e) {
        LOG.error("Actor exception", e);
        // TODO, once integrated into the larger flow of executing queries, kill the actors and report and exception to root
    }

    private Either<Request, Response> messageToSend(Request fromUpstream, ResponseProducer responseProducer) {
        while (responseProducer.hasTraversalProducer()) {
            ConceptMap conceptMap = responseProducer.traversalProducer().next();
            LOG.trace("{}: traversal answer: {}", name, conceptMap);
            if (!responseProducer.hasProduced(conceptMap)) {
                responseProducer.recordProduced(conceptMap);
                ResolutionAnswer answer = new ResolutionAnswer(fromUpstream.partialConceptMap().aggregateWith(conceptMap),
                                                               conjunction.toString(), ResolutionAnswer.Derivation.EMPTY, self(), true);
                return Either.second(new Response.Answer(fromUpstream, answer));
            }
        }

        if (responseProducer.hasDownstreamProducer()) {
            return Either.first(responseProducer.nextDownstreamProducer());
        } else {
            return Either.second(new Response.Exhausted(fromUpstream));
        }
    }


    private boolean isLast(Actor<? extends Resolver<?>> actor) {
        return plannedConcludables.get(plannedConcludables.size() - 1).first().equals(actor);
    }

    private Pair<Actor<ConcludableResolver>, Map<Reference.Name, Reference.Name>> nextPlannedDownstream(Actor<? extends Resolver<?>> actor) {
        int index = -1;
        for (int i = 0; i < plannedConcludables.size(); i++) {
            if (actor.equals(plannedConcludables.get(i).first())) { index = i; break; }
        }
        assert index != -1 && index < plannedConcludables.size() - 1 ;
        return plannedConcludables.get(index + 1);
    }
}
