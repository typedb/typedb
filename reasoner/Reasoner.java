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

import grakn.core.common.iterator.ResourceIterator;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.traversal.TraversalEngine;

import java.util.Collections;

import static grakn.common.collection.Collections.list;
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.common.iterator.Iterators.link;
import static grakn.core.common.iterator.Iterators.parallel;

public class Reasoner {

    private final TraversalEngine traversalEng;
    private final ConceptManager conceptMgr;

    public Reasoner(TraversalEngine traversalEng, ConceptManager conceptMgr) {
        this.traversalEng = traversalEng;
        this.conceptMgr = conceptMgr;
    }

    public ResourceIterator<ConceptMap> execute(Disjunction disjunction) {
        return parallel(iterate(disjunction.conjunctions()).map(this::execute).toList());
    }

    public ResourceIterator<ConceptMap> execute(Disjunction disjunction, ConceptMap bounds) {
        return parallel(iterate(disjunction.conjunctions()).map(conj -> execute(conj, bounds)).toList());
    }

    public ResourceIterator<ConceptMap> execute(Conjunction conjunction) {
        Conjunction conjunctionResolvedTypes = resolveTypes(conjunction);
        ResourceIterator<ConceptMap> answers = link(list(
                traversalEng.execute(conjunctionResolvedTypes.traversal()).map(conceptMgr::conceptMap)
//                infer(conjunctionResolvedTypes)
        ));

        if (conjunctionResolvedTypes.negations().isEmpty()) return answers;
        else return answers.filter(answer -> !parallel(iterate(conjunctionResolvedTypes.negations()).map(
                negation -> execute(negation.disjunction(), answer)
        ).toList()).hasNext());
    }

    public ResourceIterator<ConceptMap> execute(Conjunction conjunction, ConceptMap bounds) {
        return iterate(Collections.emptyIterator()); // TODO
    }

    private Conjunction resolveTypes(Conjunction conjunction) {
        return conjunction; // TODO
    }

    private ResourceIterator<ConceptMap> infer(Conjunction conjunction) {
        /*

         */
        return iterate(Collections.emptyIterator()); // TODO
    }
}
