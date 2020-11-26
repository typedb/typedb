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

import grakn.common.concurrent.actor.Actor;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.reasoner.resolution.MockTransaction;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.framework.Answer;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.map;

public abstract class Conjunction<T extends Conjunction<T>> extends Resolver<T> {
    private static final Logger LOG = LoggerFactory.getLogger(Conjunction.class);

    final List<Long> conjunction;
    Actor<ResolutionRecorder> resolutionRecorder;
    private final Long traversalSize;
    private final List<Actor<Concludable>> plannedConcludables;

    public Conjunction(Actor<T> self, String name, List<Long> conjunction, Long traversalSize) {
        super(self, name);

        this.conjunction = conjunction;
        this.traversalSize = traversalSize;
        this.plannedConcludables = new ArrayList<>();
    }

    @Override
    protected ResponseProducer createResponseProducer(Request request) {
        Iterator<ConceptMap> traversal = (new MockTransaction(traversalSize)).query(conjunction, new ConceptMap());
        ResponseProducer responseProducer = new ResponseProducer(traversal);
        Request toDownstream = new Request(request.path().append(plannedConcludables.get(0)), request.partialConceptMap(),
                request.unifiers(), new Answer.Derivation(map()));
        responseProducer.addDownstreamProducer(toDownstream);

        return responseProducer;
    }

    @Override
    protected void initialiseDownstreamActors(ResolverRegistry registry) {
        resolutionRecorder = registry.resolutionRecorder();
        List<Long> planned = list(conjunction);
        // in the future, we'll check if the atom is rule resolvable first
        for (Long atomicPattern : planned) {
            Actor<Concludable> atomicActor = registry.registerConcludable(atomicPattern, Arrays.asList(), 5L);
            plannedConcludables.add(atomicActor);
        }
    }

    boolean isLast(Actor<? extends Resolver<?>> actor) {
        return plannedConcludables.get(plannedConcludables.size() - 1).equals(actor);
    }

    Actor<Concludable> nextPlannedDownstream(Actor<? extends Resolver<?>> actor) {
        return plannedConcludables.get(plannedConcludables.indexOf(actor) + 1);
    }

    @Override
    protected void exception(Exception e) {
        LOG.error("Actor exception", e);
        // TODO, once integrated into the larger flow of executing queries, kill the actors and report and exception to root
    }
}
