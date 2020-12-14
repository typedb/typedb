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

package grakn.core.reasoner;

import grakn.core.common.concurrent.ExecutorService;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.producer.Producer;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.logic.LogicManager;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.traversal.TraversalEngine;

import java.util.List;
import java.util.function.Predicate;

import static grakn.common.collection.Collections.list;
import static grakn.core.common.concurrent.ExecutorService.PARALLELISATION_FACTOR;
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.common.producer.Producers.buffer;
import static java.util.stream.Collectors.toList;

public class Reasoner {

    private final TraversalEngine traversalEng;
    private final ConceptManager conceptMgr;
    private final LogicManager logicMgr;
    private final ResolverRegistry resolverRegistry;

    public Reasoner(ConceptManager conceptMgr, TraversalEngine traversalEng, LogicManager logicMgr) {
        this.conceptMgr = conceptMgr;
        this.traversalEng = traversalEng;
        this.logicMgr = logicMgr;
        this.resolverRegistry = new ResolverRegistry(ExecutorService.eventLoopGroup());
    }

    public ResourceIterator<ConceptMap> executeSync(Disjunction disjunction) {
        return iterate(disjunction.conjunctions()).flatMap(
                c -> traversalEng.iterator(c.traversal()).map(conceptMgr::conceptMap)
        );
    }

    public ResourceIterator<ConceptMap> execute(Disjunction disjunction) {
        return buffer(disjunction.conjunctions().stream()
                              .flatMap(conjunction -> execute(conjunction).stream())
                              .collect(toList())).iterator();
    }

    public List<Producer<ConceptMap>> execute(Disjunction disjunction, ConceptMap bounds) {
        return disjunction.conjunctions().stream().flatMap(conj -> execute(conj, bounds).stream()).collect(toList());
    }

    public List<Producer<ConceptMap>> execute(Conjunction conjunction) {
        // TODO conjunction = logicMgr.typeHinter().computeHints(conjunction, PARALLELISATION_FACTOR);
        Producer<ConceptMap> answers = traversalEng
                .producer(conjunction.traversal(), PARALLELISATION_FACTOR)
                .map(conceptMgr::conceptMap);

        // TODO enable reasoner here
        //      ResourceIterator<ConceptMap> answers = link(list(
        //          traversalEng.execute(conjunctionResolvedTypes.traversal()).map(conceptMgr::conceptMap)
        //          resolve(conjunctionResolvedTypes)
        //      ));

        if (conjunction.negations().isEmpty()) {
            return list(answers);
        } else {
            Predicate<ConceptMap> predicate = answer -> !buffer(conjunction.negations().stream().flatMap(
                    negation -> execute(negation.disjunction(), answer).stream()
            ).collect(toList())).iterator().hasNext();
            return list(answers.filter(predicate));
        }
    }

    public List<Producer<ConceptMap>> execute(Conjunction conjunction, ConceptMap bounds) {
        return null; // TODO
    }

    private ReasonerProducer resolve(Conjunction conjunction) {
        // TODO get onAnswer and onDone callbacks
        return new ReasonerProducer(conjunction, resolverRegistry);
    }
}
