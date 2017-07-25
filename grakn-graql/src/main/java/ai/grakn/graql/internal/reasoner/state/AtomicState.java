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
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.internal.reasoner.rule.RuleTuple;
import java.util.Collections;
import java.util.Iterator;

/**
 *
 */
public class AtomicState extends ResolutionState{

    private final Iterator<Answer> dbIterator;
    private final Iterator<RuleTuple> ruleIterator;

    private final QueryCache<ReasonerAtomicQuery> cache;

    public AtomicState(ReasonerAtomicQuery query, Answer sub, Unifier u, ResolutionState parent, QueryCache<ReasonerAtomicQuery> cache) {
        super(query, sub, u, parent);
        this.cache = cache;
        this.dbIterator = query.lookup(cache)
                .map(a -> a.explain(a.getExplanation().setQuery(query)))
                .iterator();
        this.ruleIterator = query.getRuleIterator();
    }

    private AtomicState(AtomicState state){
        super(state);
        this.dbIterator = Collections.emptyIterator();
        this.ruleIterator = Collections.emptyIterator();
        this.cache = state.cache;
    }

    @Override
    public ResolutionState copy() {
        return new AtomicState(this);
    }

    @Override
    public ResolutionState generateSubGoal() {
        if (dbIterator.hasNext())
            return new AnswerState(getQuery(), dbIterator.next(), getUnifier(), this);

        if(ruleIterator.hasNext())
            return generateSubGoalFromRule(ruleIterator.next());
        return null;
    }

    @Override
    public boolean isAtomicState() { return true;}

    private ResolutionState generateSubGoalFromRule(RuleTuple ruleTuple){
        InferenceRule rule = ruleTuple.getRule();
        Unifier ruleUnifier = ruleTuple.getRuleUnifier();
        Unifier permutationUnifier = ruleTuple.getPermutationUnifier();

        //delta' = theta . thetaP . delta
        Answer sub = getQuery().getSubstitution();
        Unifier uInv = ruleUnifier.inverse();
        Answer partialSubPrime = sub
                .unify(permutationUnifier)
                .unify(uInv);

        Unifier combinedUnifier = ruleUnifier.combine(permutationUnifier);
        return rule.getBody().subGoal(partialSubPrime, combinedUnifier, this, cache);
    }
}
