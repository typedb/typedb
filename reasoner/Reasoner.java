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

package grakn.core.reasoner;

import grakn.core.common.concurrent.ExecutorService;
import grakn.core.common.concurrent.actor.Actor;
import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.producer.Producer;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.logic.LogicManager;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.pattern.Negation;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.traversal.TraversalEngine;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static grakn.common.collection.Collections.list;
import static grakn.core.common.concurrent.ExecutorService.PARALLELISATION_FACTOR;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.ThingRead.CONTRADICTORY_BOUND_VARIABLE;
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.common.producer.Producers.queue;
import static java.util.stream.Collectors.toList;

public class Reasoner {

    private final TraversalEngine traversalEng;
    private final ConceptManager conceptMgr;
    private final LogicManager logicMgr;
    private final ResolverRegistry resolverRegistry;
    private final Actor<ResolutionRecorder> resolutionRecorder; // for explanations

    public Reasoner(TraversalEngine traversalEng, ConceptManager conceptMgr, LogicManager logicMgr) {
        this.conceptMgr = conceptMgr;
        this.traversalEng = traversalEng;
        this.logicMgr = logicMgr;
        this.resolutionRecorder = Actor.create(ExecutorService.eventLoopGroup(), ResolutionRecorder::new);
        this.resolverRegistry = new ResolverRegistry(ExecutorService.eventLoopGroup(), resolutionRecorder, traversalEng, conceptMgr, logicMgr);
    }

    public ResourceIterator<ConceptMap> execute(Disjunction disjunction, boolean isParallel) {
        Set<Conjunction> conjunctions = disjunction.conjunctions().stream()
                .map(conjunction -> logicMgr.typeResolver().resolveLabels(conjunction))
                .map(conjunction -> logicMgr.typeResolver().resolveVariablesExhaustive(conjunction))
                .collect(Collectors.toSet());
        // TODO enable: conjunction = logicMgr.typeResolver().resolveVariablesExhaustive(conjunction);
        if (!isParallel) return iterate(conjunctions).flatMap(this::iterator);
        else return queue(conjunctions.stream()
                                  .flatMap(conjunction -> producers(conjunction).stream())
                                  .collect(toList())).iterator();
    }

    private List<Producer<ConceptMap>> producers(Conjunction conjunction) {
        Producer<ConceptMap> answers = traversalEng
                .producer(conjunction.traversal(), PARALLELISATION_FACTOR)
                .map(conceptMgr::conceptMap);

        // TODO enable reasoner here

        Set<Negation> negations = conjunction.negations();
        if (negations.isEmpty()) return list(answers);
        else {
            Predicate<ConceptMap> predicate = answer -> !queue(iterate(negations).flatMap(
                    n -> iterate(producers(n.disjunction(), answer))).toList()
            ).iterator().hasNext();
            return list(answers.filter(predicate));
        }
    }

    private List<Producer<ConceptMap>> producers(Disjunction disjunction, ConceptMap bounds) {
        return iterate(disjunction.conjunctions()).flatMap(conj -> iterate(producers(conj, bounds))).toList();
    }

    public List<Producer<ConceptMap>> producers(Conjunction conjunction, ConceptMap bounds) {
        return producers(bound(conjunction, bounds));
    }

    private ResourceIterator<ConceptMap> iterator(Conjunction conjunction) {
        ResourceIterator<ConceptMap> answers = traversalEng.iterator(conjunction.traversal()).map(conceptMgr::conceptMap);

        // TODO: enable reasoner here
        //       ResourceIterator<ConceptMap> answers = link(answers, resolve(conjunctionResolvedTypes));

        Set<Negation> negations = conjunction.negations();
        if (negations.isEmpty()) return answers;
        else return answers.filter(answer -> !iterate(negations).flatMap(
                negation -> iterator(negation.disjunction(), answer)
        ).hasNext());
    }

    private ResourceIterator<ConceptMap> iterator(Disjunction disjunction, ConceptMap bounds) {
        return iterate(disjunction.conjunctions()).flatMap(c -> iterator(c, bounds));
    }

    private ResourceIterator<ConceptMap> iterator(Conjunction conjunction, ConceptMap bounds) {
        return iterator(bound(conjunction, bounds));
    }

    private Conjunction bound(Conjunction conjunction, ConceptMap bounds) {
        Conjunction newClone = conjunction.clone();
        newClone.forEach(var -> {
            if (var.id().isNamedReference() && bounds.contains(var.id().reference().asName())) {
                Concept boundVar = bounds.get(var.id().reference().asName());
                if (var.isType() != boundVar.isType()) throw GraknException.of(CONTRADICTORY_BOUND_VARIABLE, var);
                else if (var.isType()) var.asType().label(boundVar.asType().getLabel());
                else if (var.isThing()) var.asThing().iid(boundVar.asThing().getIID());
                else throw GraknException.of(ILLEGAL_STATE);
            }
        });
        return newClone;
    }

    ResolverRegistry resolverRegistry() {
        return resolverRegistry;
    }

    private ReasonerProducer resolve(Conjunction conjunction) {
        return new ReasonerProducer(conjunction, resolverRegistry);
    }
}
