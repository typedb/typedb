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

    private final Set<Answer> answers;

    public Explanation(){ answers = new HashSet<>();}
    public Explanation(Set<Answer> ans){ answers = ans;}
    public Explanation(Explanation e){ answers = new HashSet<>(e.answers);}

    public AnswerExplanation copy(){ return new Explanation(this);}

    @Override
    public boolean addAnswer(Answer a){ return answers.add(a);}

    @Override
    public Set<Answer> getAnswers(){ return answers;}

    @Override
    public boolean isLookupExplanation(){ return false;}

    @Override
    public boolean isRuleExplanation(){ return false;}

    public ReasonerQuery getQuery(){ return null;}
}
