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

package ai.grakn.graql.internal.reasoner.explanation;

import ai.grakn.graql.admin.ConceptMap;
import ai.grakn.graql.admin.Explanation;
import ai.grakn.graql.admin.ReasonerQuery;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * <p>
 * Base class for explanation classes.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class QueryExplanation implements Explanation {

    private final ReasonerQuery query;
    private final ImmutableList<ConceptMap> answers;

    public QueryExplanation(){
        this.query = null;
        this.answers = ImmutableList.of();}
    QueryExplanation(ReasonerQuery q, List<ConceptMap> ans){
        this.query = q;
        this.answers = ImmutableList.copyOf(ans);
    }
    QueryExplanation(ReasonerQuery q){
        this(q, new ArrayList<>());
    }
    QueryExplanation(List<ConceptMap> ans){
        this(null, ans);
    }

    @Override
    public ai.grakn.graql.admin.Explanation setQuery(ReasonerQuery q){
        return new QueryExplanation(q);
    }

    @Override
    public ai.grakn.graql.admin.Explanation childOf(ConceptMap ans) {
        return new QueryExplanation(getQuery(), ans.explanation().getAnswers());
    }

    @Override
    public ImmutableList<ConceptMap> getAnswers(){ return answers;}

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
