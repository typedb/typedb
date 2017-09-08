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

package ai.grakn.graql.internal.reasoner.state;

import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.explanation.RuleExplanation;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import java.util.Set;


/**
 *
 * <p>
 * Resolution state holding an answer ({@link Answer}) to the parent state.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
class AnswerState extends ResolutionState {

    AnswerState(Answer sub, Unifier u, QueryState parent) {
        super(sub, u, parent);
    }

    @Override
    public boolean isAnswerState(){ return true;}

    @Override
    public ResolutionState generateSubGoal() {
        return getParentState().propagateAnswer(this);
    }


    Answer getAtomicAnswer(ReasonerAtomicQuery query, InferenceRule rule, Unifier cacheUnifier, QueryCache<ReasonerAtomicQuery> cache){
        Answer answer;
        if (rule == null){
            answer = getSubstitution();
        } else {
            answer = rule.requiresMaterialisation(query.getAtom()) ?
                    getMaterialisedAnswer(query, rule, cache) :
                    getRuleAnswer(query, rule);
        }

        if (answer.isEmpty()) return answer;
        return cache.recordAnswerWithUnifier(query, answer, cacheUnifier);
    }

    private Answer getRuleAnswer(ReasonerAtomicQuery query, InferenceRule rule){
        Answer answer = getSubstitution()
                .unify(getUnifier());
        if (answer.isEmpty()) return answer;

        return answer
                .merge(query.getSubstitution())
                .filterVars(query.getVarNames())
                .explain(new RuleExplanation(query, rule));
    }

    private Answer getMaterialisedAnswer(ReasonerAtomicQuery query, InferenceRule rule, QueryCache<ReasonerAtomicQuery> cache){
        Answer ans = getSubstitution();
        Unifier unifier = getUnifier();
        ReasonerAtomicQuery ruleHead = rule.getHead();
        Set<Var> queryVars = query.getVarNames().size() < ruleHead.getVarNames().size()?
                unifier.keySet() :
                ruleHead.getVarNames();

        boolean queryEquivalentToHead = query.isEquivalent(ruleHead, Atomic::isAlphaEquivalent);

        //check if the specific answer to ruleHead already in cache/db
        Answer headAnswer = ruleHead
                            .lookupAnswer(cache, ans)
                            .filterVars(queryVars)
                            .unify(unifier);

        //if not and query different than rule head do the same with the query
        Answer queryAnswer = headAnswer.isEmpty() && queryEquivalentToHead?
                query.lookupAnswer(cache, ans) :
                new QueryAnswer();

        //ensure no duplicates created - only materialise answer if it doesn't exist in the db
        if (headAnswer.isEmpty()
                && queryAnswer.isEmpty()) {
            Answer materialisedSub = ruleHead.materialise(ans).findFirst().orElse(null);
            if (!queryEquivalentToHead) cache.recordAnswer(ruleHead, materialisedSub);
            ans = materialisedSub
                    .filterVars(queryVars)
                    .unify(unifier);
        } else {
            ans = headAnswer.isEmpty()? queryAnswer : headAnswer;
        }

        if (ans.isEmpty()) return ans;

        return ans
                .merge(query.getSubstitution())
                .explain(new RuleExplanation(query, rule));
    }

}
