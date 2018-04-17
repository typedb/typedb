/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine.controller.response;

import ai.grakn.graql.admin.AnswerExplanation;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.UnifierType;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.explanation.RuleExplanation;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * Builds the explanation for a given series of {@link Answer}s
 * </p>
 *
 * @author Marco Scoppetta
 */
public class ExplanationBuilder {

    public static List<Answer> buildExplanation(ai.grakn.graql.admin.Answer queryAnswer) {
        final List<Answer> explanation = new ArrayList<>();
        queryAnswer.getExplanation().getAnswers().forEach(answer -> {
            AnswerExplanation expl = answer.getExplanation();
            Atom atom = ((ReasonerAtomicQuery) expl.getQuery()).getAtom();
            ai.grakn.graql.admin.Answer inferredAnswer = new QueryAnswer();

            if (expl.isLookupExplanation()){
                ReasonerAtomicQuery rewrittenQuery = ReasonerQueries.atomic(atom.isResource()? atom : atom.rewriteWithRelationVariable());
                inferredAnswer = ReasonerQueries.atomic(rewrittenQuery, answer).getQuery().stream()
                        .findFirst().orElse(new QueryAnswer());
            } else if (expl.isRuleExplanation()) {
                Atom headAtom = ((RuleExplanation) expl).getRule().getHead().getAtom();
                ReasonerAtomicQuery rewrittenQuery = ReasonerQueries.atomic(headAtom.isResource()? headAtom : headAtom.rewriteWithRelationVariable());

                inferredAnswer = headAtom.getMultiUnifier(atom, UnifierType.RULE).stream()
                        .map(Unifier::inverse)
                        .flatMap(unifier -> rewrittenQuery.materialise(answer.unify(unifier)))
                        .findFirst().orElse(new QueryAnswer());
            }
            explanation.add(Answer.create(inferredAnswer));
        });
        return explanation;
    }
}
