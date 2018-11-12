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

package grakn.core.graql.internal.reasoner.explanation;

import grakn.core.graql.admin.Explanation;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.answer.ConceptMap;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * Base class for explanation classes.
 * </p>
 *
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
    public Explanation setQuery(ReasonerQuery q){
        return new QueryExplanation(q);
    }

    @Override
    public Explanation childOf(ConceptMap ans) {
        return new QueryExplanation(getQuery(), ans.explanation().getAnswers());
    }

    @Override
    public ImmutableList<ConceptMap> getAnswers(){ return answers;}

    @Override
    public Set<ConceptMap> explicit(){
        return deductions().stream().filter(ans -> ans.explanation().isLookupExplanation()).collect(Collectors.toSet());
    }

    @Override
    public Set<ConceptMap> deductions(){
        Set<ConceptMap> answers = new HashSet<>(this.getAnswers());
        this.getAnswers().forEach(ans -> answers.addAll(ans.explanation().deductions()));
        return answers;
    }

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
