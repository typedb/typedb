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

package ai.grakn.graql.internal.reasoner.explanation;

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.AnswerExplanation;
import ai.grakn.graql.admin.ReasonerQuery;
import com.google.common.collect.ImmutableSet;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * <p>
 * Base class for explanation classes.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class Explanation implements AnswerExplanation {

    private final ReasonerQuery query;
    private final ImmutableSet<Answer> answers;

    public Explanation(){
        this.query = null;
        this.answers = ImmutableSet.of();}
    Explanation(ReasonerQuery q, Set<Answer> ans){
        this.query = q;
        this.answers = ImmutableSet.copyOf(ans);
    }
    Explanation(ReasonerQuery q){
        this(q, new HashSet<>());
    }
    Explanation(Set<Answer> ans){
        this(null, ans);
    }

    @Override
    public AnswerExplanation setQuery(ReasonerQuery q){
        return new Explanation(q);
    }

    @Override
    public AnswerExplanation withAnswers(Set<Answer> answers) {
        return new Explanation(getQuery(), answers);
    }

    @Override
    public ImmutableSet<Answer> getAnswers(){ return answers;}

    @Override
    public boolean isLookupExplanation(){ return false;}

    @Override
    public boolean isRuleExplanation(){ return false;}

    @Override
    public boolean isJoinExplanation(){ return false;}

    @Override
    public boolean isEmpty() { return !isLookupExplanation() && !isRuleExplanation() && getAnswers().isEmpty();}

    @Override
    public ReasonerQuery getQuery(){ return query;}
}
