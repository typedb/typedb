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
import grakn.core.common.iterator.Iterators;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Context;
import grakn.core.common.producer.Producer;
import grakn.core.common.producer.Producers;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static grakn.common.collection.Collections.list;
import static grakn.core.common.concurrent.ExecutorService.PARALLELISATION_FACTOR;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.ThingRead.CONTRADICTORY_BOUND_VARIABLE;
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.common.iterator.Iterators.link;
import static grakn.core.common.producer.Producers.iterable;
import static java.util.stream.Collectors.toList;

public class Reasoner {
    private static final Logger LOG = LoggerFactory.getLogger(Reasoner.class);

    private final TraversalEngine traversalEng;
    private final ConceptManager conceptMgr;
    private final LogicManager logicMgr;
    private Context.Transaction context;
    private final ResolverRegistry resolverRegistry;
    private final Actor<ResolutionRecorder> resolutionRecorder; // for explanations

    public Reasoner(TraversalEngine traversalEng, ConceptManager conceptMgr, LogicManager logicMgr, Context.Transaction context) {
        this.conceptMgr = conceptMgr;
        this.traversalEng = traversalEng;
        this.logicMgr = logicMgr;
        this.context = context;
        this.resolutionRecorder = Actor.create(ExecutorService.eventLoopGroup(), ResolutionRecorder::new);
        this.resolverRegistry = new ResolverRegistry(ExecutorService.eventLoopGroup(), resolutionRecorder, traversalEng, conceptMgr, logicMgr);
    }

    public ResourceIterator<ConceptMap> execute(Disjunction disjunction, boolean isParallel) {
        Set<Conjunction> conjunctions = disjunction.conjunctions().stream()
                .map(conjunction -> logicMgr.typeResolver().resolve(conjunction))
                .collect(Collectors.toSet());
        if (!isParallel) return iterate(conjunctions).flatMap(this::iterator);
        else return iterable(conjunctions.stream()
                                     .flatMap(conjunction -> producers(conjunction).stream())
                                     .collect(toList())).iterator();
    }

    private List<Producer<ConceptMap>> producers(Conjunction conjunction) {
        Producer<ConceptMap> answers = traversalEng
                .producer(conjunction.traversal(), PARALLELISATION_FACTOR)
                .map(conceptMgr::conceptMap);

        boolean reasonerEnabled = true;
        if (context.sessionType().isSchema() && context.transactionType().isWrite()) {
            LOG.warn("Reasoning is disabled in schema write transactions");
            reasonerEnabled = false;
        }
        // TODO check query options if reasoner is disabled

        Set<Negation> negations = conjunction.negations();
        if (negations.isEmpty()) {
            return reasonerEnabled ? list(answers, resolve(conjunction)) : list(answers);
        }
        else {
            Predicate<ConceptMap> predicate = answer -> !iterable(iterate(negations).flatMap(
                    n -> iterate(producers(n.disjunction(), answer))).toList()
            ).iterator().hasNext();
            return reasonerEnabled ?
                    list(answers.filter(predicate), resolve(conjunction).filter(predicate)) : // TODO remove filter after negation implemented in reasoner
                    list(answers.filter(predicate));
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

        boolean reasonerEnabled = true;
        if (context.sessionType().isSchema() && context.transactionType().isWrite()) {
            LOG.warn("Reasoning is disabled in schema write transactions");
            reasonerEnabled = false;
        }
        // TODO check query options if reasoner is disabled
        ResourceIterator<ConceptMap> reasonerAnswers = reasonerEnabled ? iterable(resolve(conjunction)).iterator() : Iterators.empty();

        // TODO remove reasoner answers negation filter after negation implemented in reasoner
        Set<Negation> negations = conjunction.negations();
        if (negations.isEmpty()) return link(answers, reasonerAnswers);
        else return link(answers, reasonerAnswers).filter(answer -> !iterate(negations).flatMap(
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

    private Producer<ConceptMap> resolve(Conjunction conjunction) {
        return Producers.empty();
        // TODO enable reasoner when ready!
        // return new ReasonerProducer(conjunction, resolverRegistry);
    }
}
