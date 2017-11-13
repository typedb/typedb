/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.hal;

import ai.grakn.Keyspace;
import ai.grakn.concept.Concept;
import ai.grakn.graql.Match;
import ai.grakn.graql.Printer;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.AnswerExplanation;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.UnifierType;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.explanation.RuleExplanation;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import com.google.common.collect.Iterables;

import mjson.Json;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * Class for building HAL representations of a {@link Concept} or a {@link Match}.
 *
 * @author Marco Scoppetta
 */
public class HALBuilder {

    public static String renderHALConceptData(Concept concept, boolean inferred, int separationDegree, Keyspace keyspace, int offset, int limit) {
        return new HALConceptData(concept, inferred, separationDegree, false, new HashSet<>(), keyspace, offset, limit).render();
    }

    @Nullable
    public static String HALExploreConcept(Concept concept, Keyspace keyspace, int offset, int limit) {
        String renderedHAL = null;

        if (concept.isThing()) {
            renderedHAL = new HALExploreInstance(concept, keyspace, offset, limit).render();
        }
        if (concept.isSchemaConcept()) {
            renderedHAL = new HALExploreSchemaConcept(concept, keyspace, offset, limit).render();
        }

        return renderedHAL;
    }

    public static Json explanationAnswersToHAL(Collection<Answer> answers, Printer halPrinter) {
        final Json conceptsArray = Json.array();
        answers.forEach(answer -> {
            AnswerExplanation expl = answer.getExplanation();
            Atom atom = ((ReasonerAtomicQuery) expl.getQuery()).getAtom();
            ReasonerAtomicQuery rewrittenQuery = ReasonerQueries.atomic(atom.rewriteWithRelationVariable()).withSubstitution(answer);

            List<Answer> userDefinedAnswers = rewrittenQuery.getQuery().execute();
            Answer inferredAnswer = new QueryAnswer();

            if (!userDefinedAnswers.isEmpty()) {
                inferredAnswer = Iterables.getOnlyElement(userDefinedAnswers);

            } else if (expl.isRuleExplanation()) {
                Atom headAtom = ((RuleExplanation) expl).getRule().getHead().getAtom();

                inferredAnswer = headAtom.getMultiUnifier(atom, UnifierType.RULE).stream()
                            .map(Unifier::inverse)
                            .flatMap(unifier -> new ReasonerAtomicQuery(headAtom.rewriteWithRelationVariable()).materialise(answer.unify(unifier)))
                            .findFirst().orElse(new QueryAnswer());

            }
            conceptsArray.add(halPrinter.graqlString(false, inferredAnswer));

        });
        return conceptsArray;
    }

}