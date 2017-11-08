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
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;

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
public class AtomicState extends QueryState<ReasonerAtomicQuery>{

    public AtomicState(ReasonerAtomicQuery q,
                       Answer sub,
                       Unifier u,
                       QueryStateBase parent,
                       Set<ReasonerAtomicQuery> subGoals,
                       QueryCache<ReasonerAtomicQuery> cache) {
        super(ReasonerQueries.atomic(q, sub), sub, u, parent, subGoals, cache);
    }

    @Override
    boolean isAtomicState(){ return true;}

    @Override
    ResolutionState propagateAnswer(AnswerState state){
        Answer answer = state.getAnswer();
        if (answer.isEmpty()) return null;

        if (state.getRule() != null && getQuery().getAtom().requiresRoleExpansion()){
            return new RoleExpansionState(answer, getUnifier(), getQuery().getAtom().getRoleExpansionVariables(), getParentState());
        }

        return new AnswerState(answer, getUnifier(), getParentState());
    }

}
