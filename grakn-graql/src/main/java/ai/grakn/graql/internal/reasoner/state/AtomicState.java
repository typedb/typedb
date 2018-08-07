/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package ai.grakn.graql.internal.reasoner.state;

import ai.grakn.graql.Var;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.query.answer.ConceptMapImpl;
import ai.grakn.graql.internal.reasoner.cache.SimpleQueryCache;
import ai.grakn.graql.internal.reasoner.explanation.RuleExplanation;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;

import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Set;

/**
 *
 * <p>
 * Query state corresponding to an atomic query ({@link ReasonerAtomicQuery}) in the resolution tree.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
@SuppressFBWarnings("BC_UNCONFIRMED_CAST_OF_RETURN_VALUE")
class AtomicState extends QueryState<ReasonerAtomicQuery>{

    AtomicState(ReasonerAtomicQuery query,
                ConceptMap sub,
                Unifier u,
                QueryStateBase parent,
                Set<ReasonerAtomicQuery> subGoals,
                SimpleQueryCache<ReasonerAtomicQuery> cache) {
        super(ReasonerQueries.atomic(query, sub),
                sub,
                u,
                () -> cache.getAnswerStreamWithUnifier(ReasonerQueries.atomic(query, sub)).getValue().inverse(),
                parent,
                subGoals,
                cache);
    }

    @Override
    ResolutionState propagateAnswer(AnswerState state){
        ConceptMap answer = state.getAnswer();
        ReasonerAtomicQuery query = getQuery();
        if (answer.isEmpty()) return null;

        if (state.getRule() != null && query.getAtom().requiresRoleExpansion()){
            return new RoleExpansionState(answer, getUnifier(), query.getAtom().getRoleExpansionVariables(), getParentState());
        }
        return new AnswerState(answer, getUnifier(), getParentState());
    }

    @Override
    ConceptMap consumeAnswer(AnswerState state) {
        ConceptMap answer;
        ReasonerAtomicQuery query = getQuery();
        ConceptMap baseAnswer = state.getSubstitution();
        InferenceRule rule = state.getRule();
        Unifier unifier = state.getUnifier();
        if (rule == null) answer = state.getSubstitution();
        else{
            answer = rule.requiresMaterialisation(query.getAtom()) ?
                    materialisedAnswer(baseAnswer, rule, unifier) :
                    ruleAnswer(baseAnswer, rule, unifier);
        }
        return getCache().record(query, answer, getCacheUnifier());
    }

    private ConceptMap ruleAnswer(ConceptMap baseAnswer, InferenceRule rule, Unifier unifier){
        ReasonerAtomicQuery query = getQuery();
        ConceptMap answer = baseAnswer
                .merge(rule.getHead().getRoleSubstitution())
                .unify(unifier);
        if (answer.isEmpty()) return answer;

        return answer
                .merge(query.getSubstitution())
                .project(query.getVarNames())
                .explain(new RuleExplanation(query, rule));
    }

    private ConceptMap materialisedAnswer(ConceptMap baseAnswer, InferenceRule rule, Unifier unifier){
        ConceptMap answer = baseAnswer;
        ReasonerAtomicQuery query = getQuery();
        SimpleQueryCache<ReasonerAtomicQuery> cache = getCache();

        ReasonerAtomicQuery subbedQuery = ReasonerQueries.atomic(query, answer);
        ReasonerAtomicQuery ruleHead = ReasonerQueries.atomic(rule.getHead(), answer);

        Set<Var> queryVars = query.getVarNames().size() < ruleHead.getVarNames().size()?
                unifier.keySet() :
                ruleHead.getVarNames();

        boolean queryEquivalentToHead = subbedQuery.isEquivalent(ruleHead);

        //check if the specific answer to ruleHead already in cache/db
        ConceptMap headAnswer = cache
                .getAnswer(ruleHead, answer)
                .project(queryVars)
                .unify(unifier);

        //if not and query different than rule head do the same with the query
        ConceptMap queryAnswer = headAnswer.isEmpty() && queryEquivalentToHead?
                cache.getAnswer(query, answer) :
                new ConceptMapImpl();

        //ensure no duplicates created - only materialise answer if it doesn't exist in the db
        if (headAnswer.isEmpty()
                && queryAnswer.isEmpty()) {
            ConceptMap materialisedSub = ruleHead.materialise(answer).findFirst().orElse(null);
            if (!queryEquivalentToHead) cache.record(ruleHead, materialisedSub);
            answer = materialisedSub
                    .project(queryVars)
                    .unify(unifier);
        } else {
            answer = headAnswer.isEmpty()? queryAnswer : headAnswer;
        }

        if (answer.isEmpty()) return answer;

        return answer
                .merge(query.getSubstitution())
                .explain(new RuleExplanation(query, rule));
    }
}
