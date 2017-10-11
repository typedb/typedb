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

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.internal.reasoner.utils.Pair;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Query state corresponding to an atomic query ({@link ReasonerAtomicQuery}) in the resolution tree.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class AtomicState extends QueryState{

    private final ReasonerAtomicQuery query;
    private final Iterator<Answer> dbIterator;
    private final Iterator<Pair<InferenceRule, Unifier>> ruleIterator;

    private InferenceRule currentRule = null;
    private final MultiUnifier cacheUnifier;

    public AtomicState(ReasonerAtomicQuery q,
                       Answer sub,
                       Unifier u,
                       QueryState parent,
                       Set<ReasonerAtomicQuery> subGoals,
                       QueryCache<ReasonerAtomicQuery> cache) {

        super(sub, u, parent, subGoals, cache);
        this.query = ReasonerQueries.atomic(q, sub);

        Pair<Stream<Answer>, MultiUnifier> streamUnifierPair = cache.getAnswerStreamWithUnifier(query);
        this.dbIterator = streamUnifierPair.getKey()
                .map(a -> a.explain(a.getExplanation().setQuery(query)))
                .iterator();
        this.cacheUnifier = streamUnifierPair.getValue().inverse();

        //if this is ground and exists in the db then do not resolve further
        if(subGoals.contains(query)
                || (query.isGround() && dbIterator.hasNext() ) ){
            this.ruleIterator = Collections.emptyIterator();
        } else {
            this.ruleIterator = query.getRuleIterator();
        }

        //mark as visited and hence not admissible
        if (ruleIterator.hasNext()) subGoals.add(query);
    }

    @Override
    boolean isAtomicState(){ return true;}

    ResolutionState propagateAnswer(AnswerState state){
        Answer answer = state.getAnswer();
        if (answer.isEmpty()) return null;

        if (currentRule != null && query.getAtom().requiresRoleExpansion()){
            return new RoleExpansionState(answer, getUnifier(), query.getAtom().getRoleExpansionVariables(), getParentState());
        }

        return new AnswerState(answer, getUnifier(), getParentState());
    }

    @Override
    public ResolutionState generateSubGoal() {
        if (dbIterator.hasNext()){
            return new AnswerState(dbIterator.next(), getUnifier(), this);
        }
        if (ruleIterator.hasNext()) return generateSubGoalFromRule(ruleIterator.next());
        return null;
    }

    @Override
    ReasonerAtomicQuery getQuery(){return query;}

    @Override
    MultiUnifier getCacheUnifier(){ return cacheUnifier;}

    InferenceRule getCurrentRule(){ return currentRule;}

    private ResolutionState generateSubGoalFromRule(Pair<InferenceRule, Unifier> rulePair){
        Unifier ruleUnifier = rulePair.getValue();
        Unifier ruleUnifierInverse = ruleUnifier.inverse();

        //delta' = theta . thetaP . delta
        Answer partialSubPrime = query.getSubstitution()
                .unify(ruleUnifierInverse);

        currentRule = rulePair.getKey()
                .propagateConstraints(query.getAtom(), ruleUnifierInverse);

        return currentRule.getBody().subGoal(partialSubPrime, ruleUnifier, this, getSubGoals(), getCache());
    }
}
