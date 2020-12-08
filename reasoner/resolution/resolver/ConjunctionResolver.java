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
import grakn.core.logic.concludable.ConjunctionConcludable;
import grakn.core.pattern.Conjunction;
import grakn.core.reasoner.resolution.MockTransaction;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.logic.transform.TransformedConceptMap;
import grakn.core.logic.transform.VariableMapper;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.map;

public abstract class ConjunctionResolver<T extends ConjunctionResolver<T>> extends Resolver<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ConjunctionResolver.class);

    final Conjunction conjunction;
    private final Set<ConjunctionConcludable<?, ?>> conjunctionConcludables;
    Actor<ResolutionRecorder> resolutionRecorder;
    private final List<Pair<Actor<ConcludableResolver>, VariableMapper>> plannedConcludables;

    public ConjunctionResolver(Actor<T> self, String name, Conjunction conjunction, Set<ConjunctionConcludable<?, ?>> conjunctionConcludables) {
        super(self, name);

        this.conjunction = conjunction;
        this.conjunctionConcludables = conjunctionConcludables;
        this.plannedConcludables = new ArrayList<>();
    }

    @Override
    protected ResponseProducer createResponseProducer(Request request) {
        Iterator<ConceptMap> traversal = (new MockTransaction(5L)).query(conjunction, new ConceptMap());
        ResponseProducer responseProducer = new ResponseProducer(traversal);
        Request toDownstream = new Request(request.path().append(plannedConcludables.get(0).first()), TransformedConceptMap.of(request.partialConceptMap().map(), plannedConcludables.get(0).second()),
                                           new ResolutionAnswer.Derivation(map()));
        responseProducer.addDownstreamProducer(toDownstream);

        return responseProducer;
    }

    @Override
    protected void initialiseDownstreamActors(ResolverRegistry registry) {
        resolutionRecorder = registry.resolutionRecorder();
        // Build the concludables for this conjunction

        // TODO Find the applicable rules for each, which requires 1 or more valid unifications with a rule.
        // TODO Mark concludables with no applicable rules as inconcludable
        // TODO Return to the conjunction now knowing the set of inconcludable constraints
        // TODO Tell the concludables to extend themselves by traversing all inconcludable constraints

        // Plan the order in which to execute the concludables
        List<ConjunctionConcludable<?, ?>> planned = list(conjunctionConcludables); // TODO Do some actual planning
        for (ConjunctionConcludable<?, ?> concludable : planned) {
            Pair<Actor<ConcludableResolver>, VariableMapper> concludableUnifierPair = registry.registerConcludable(concludable); // TODO TraversalAnswerCount and Rules?
            plannedConcludables.add(concludableUnifierPair);
        }
    }

    boolean isLast(Actor<? extends Resolver<?>> actor) {
        return plannedConcludables.get(plannedConcludables.size() - 1).first().equals(actor);
    }

    Pair<Actor<ConcludableResolver>, VariableMapper> nextPlannedDownstream(Actor<? extends Resolver<?>> actor) {
        boolean match = false;
        for (Pair<Actor<ConcludableResolver>, VariableMapper> planned : plannedConcludables) {
            if (match) {
                return planned; // TODO This logic seems a bit bizarre, but is the most efficient
            }
            if (actor.equals(planned.first())) {
                match = true;
            }
        }
        assert false; // Catch the case where we can't find the given actor in the plan
        return null;
    }

    @Override
    public Either<Request, Response> receiveAnswer(Request fromUpstream, Response.Answer fromDownstream, ResponseProducer responseProducer) {
        Actor<? extends Resolver<?>> sender = fromDownstream.sourceRequest().receiver();
        ConceptMap conceptMap = fromDownstream.sourceRequest().partialConceptMap().merge(fromDownstream.answer().conceptMap()).unTransform();

        ResolutionAnswer.Derivation derivation = fromDownstream.sourceRequest().partialResolutions();
        if (fromDownstream.answer().isInferred()) {
            derivation = derivation.withAnswer(fromDownstream.sourceRequest().receiver(), fromDownstream.answer());
        }

        if (isLast(sender)) {
            LOG.trace("{}: has produced: {}", name, conceptMap);

            if (!responseProducer.hasProduced(conceptMap)) {
                responseProducer.recordProduced(conceptMap);

                ResolutionAnswer answer = new ResolutionAnswer(conceptMap, conjunction.toString(), derivation, self());
                return Either.second(createResponse(fromUpstream, answer));
            } else {
                return produceMessage(fromUpstream, responseProducer);
            }
        } else {
            Pair<Actor<ConcludableResolver>, VariableMapper> nextPlannedDownstream = nextPlannedDownstream(sender);
            Request downstreamRequest = new Request(fromUpstream.path().append(nextPlannedDownstream.first()),
                                                    TransformedConceptMap.of(conceptMap, nextPlannedDownstream.second()), derivation);
            responseProducer.addDownstreamProducer(downstreamRequest);
            return Either.first(downstreamRequest);
        }
    }

    abstract Either<Request, Response> produceMessage(Request fromUpstream, ResponseProducer responseProducer);

    abstract Response createResponse(Request fromUpstream, final ResolutionAnswer answer);

    @Override
    protected void exception(Exception e) {
        LOG.error("Actor exception", e);
        // TODO, once integrated into the larger flow of executing queries, kill the actors and report and exception to root
    }
}
