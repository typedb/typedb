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

package ai.grakn.engine.controller.response;

import ai.grakn.graql.Printer;
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

    public static List<Answer> buildExplanation(ai.grakn.graql.admin.Answer queryAnswer, Printer printer) {
        final List<Answer> explanation = new ArrayList<>();
        queryAnswer.getExplanation().getAnswers().forEach(answer -> {
            AnswerExplanation expl = answer.getExplanation();
            Atom atom = ((ReasonerAtomicQuery) expl.getQuery()).getAtom();
            List<ai.grakn.graql.admin.Answer> userDefinedAnswers = ReasonerQueries.atomic(atom.rewriteWithRelationVariable()).getQuery().execute();
            ai.grakn.graql.admin.Answer inferredAnswer = new QueryAnswer();

            if (!userDefinedAnswers.isEmpty()) {
                inferredAnswer = userDefinedAnswers.get(0);
            } else if (expl.isRuleExplanation()) {
                Atom headAtom = ((RuleExplanation) expl).getRule().getHead().getAtom();

                inferredAnswer = headAtom.getMultiUnifier(atom, UnifierType.RULE).stream()
                        .map(Unifier::inverse)
                        .flatMap(unifier -> new ReasonerAtomicQuery(headAtom.rewriteWithRelationVariable()).materialise(answer.unify(unifier)))
                        .findFirst().orElse(new QueryAnswer());

            }
            explanation.add((Answer) printer.graqlString(false, inferredAnswer));
        });
        return explanation;
    }
}
