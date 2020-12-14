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
import grakn.core.logic.resolvable.Concludable;
import grakn.core.logic.resolvable.Resolvable;
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

public abstract class ConjunctionResolver<T extends ConjunctionResolver<T>> extends Resolver<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ConjunctionResolver.class);

    final Conjunction conjunction;
    private final Set<Concludable<?>> concludables;
    Actor<ResolutionRecorder> resolutionRecorder;
    private final List<Pair<Actor<? extends ResolvableResolver<?>>, Map<Reference.Name, Reference.Name>>> plan;

    public ConjunctionResolver(Actor<T> self, String name, Conjunction conjunction, Set<Concludable<?>> concludables) {
        super(self, name);

        this.conjunction = conjunction;
        this.concludables = concludables;
        this.plan = new ArrayList<>();
    }

    @Override
    protected ResponseProducer createResponseProducer(Request request) {
        Iterator<ConceptMap> traversal = (new MockTransaction(3L)).query(conjunction, new ConceptMap());
        ResponseProducer responseProducer = new ResponseProducer(traversal);
        Request toDownstream = new Request(request.path().append(plan.get(0).first()),
                                           MappingAggregator.of(request.partialConceptMap().map(), plan.get(0).second()),
                                           new ResolutionAnswer.Derivation(map()));
        responseProducer.addDownstreamProducer(toDownstream);

        return responseProducer;
    }

    @Override
    protected void initialiseDownstreamActors(ResolverRegistry registry) {
        resolutionRecorder = registry.resolutionRecorder();
        Set<Resolvable> resolvables = Resolvable.split(conjunction, concludables);

        // TODO Plan the order in which to execute the concludables
        List<Resolvable> plan = list(resolvables);
        for (Resolvable planned : plan) {
            Pair<Actor<? extends ResolvableResolver<?>>, Map<Reference.Name, Reference.Name>> concludableUnifierPair = registry.registerResolvable(planned);
            this.plan.add(concludableUnifierPair);
        }
    }

    boolean isLast(Actor<? extends Resolver<?>> actor) {
        return plan.get(plan.size() - 1).first().equals(actor);
    }

    Pair<Actor<? extends ResolvableResolver<?>>, Map<Reference.Name, Reference.Name>> nextPlannedDownstream(Actor<? extends Resolver<?>> actor) {
        int index = -1;
        for (int i = 0; i < plan.size(); i++) {
            if (actor.equals(plan.get(i).first())) { index = i; break; }
        }
        assert index != -1 && index < plan.size() - 1 ;
        return plan.get(index + 1);
    }

    abstract Either<Request, Response> messageToSend(Request fromUpstream, ResponseProducer responseProducer);

    abstract Response createResponse(Request fromUpstream, final ResolutionAnswer answer);

    @Override
    protected void exception(Exception e) {
        LOG.error("Actor exception", e);
        // TODO, once integrated into the larger flow of executing queries, kill the actors and report and exception to root
    }
}
