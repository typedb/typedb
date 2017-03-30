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

package ai.grakn.graql.internal.reasoner.query;

import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Wrapper class for a set of answers providing higher level filtering facilities
 * as well as unification operation.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class QueryAnswers implements Iterable<Answer>{

    private static final long serialVersionUID = -8092703897236995422L;

    private final HashSet<Answer> set = new HashSet<>();

    @Override
    public Iterator<Answer> iterator() { return set.iterator();}

    @Override
    public boolean equals(Object obj){
        if (obj == this) return true;
        if (obj == null || !(obj instanceof QueryAnswers)) return false;
        QueryAnswers a2 = (QueryAnswers) obj;
        return set.equals(a2.set);
    }

    @Override
    public int hashCode(){return set.hashCode();}

    public Stream<Answer> stream(){ return set.stream();}

    public QueryAnswers(){}
    public QueryAnswers(Answer ans){ set.add(ans);}
    public QueryAnswers(Collection<Answer> ans){ ans.forEach(set::add);}
    public QueryAnswers(QueryAnswers ans){ ans.forEach(set::add);}

    public boolean add(Answer a){ return set.add(a);}
    public boolean addAll(QueryAnswers ans){ return set.addAll(ans.set);}
    public boolean remove(Answer a){ return set.remove(a);}
    public boolean removeAll(QueryAnswers ans){ return set.removeAll(ans.set);}

    public boolean containsAll(QueryAnswers ans){ return set.containsAll(ans.set);}

    public int size(){ return set.size();}
    public boolean isEmpty(){ return set.isEmpty();}

    /**
     * filter answers by constraining the variable set to the provided one
     * @param vars set of variable names
     * @return filtered answers
     */
    public QueryAnswers filterVars(Set<VarName> vars) {
        return new QueryAnswers(this.stream().map(result -> Maps.filterKeys(result.map(), vars::contains))
                .map(QueryAnswer::new)
                .collect(Collectors.toSet()));
    }

    /**
     * unify the answers by applying unifiers to variable set
     * @param unifier map of [key: from/value: to] unifiers
     * @return unified query answers
     */
    public QueryAnswers unify(Unifier unifier){
        if (unifier.isEmpty()) return new QueryAnswers(this);
        QueryAnswers unifiedAnswers = new QueryAnswers();
        this.forEach(answer -> {
            Answer unifiedAnswer = answer.unify(unifier);
            unifiedAnswers.add(unifiedAnswer);
        });

        return unifiedAnswers;
    }

    /**
     * unify answers of childQuery with parentQuery
     * @param parentQuery parent atomic query containing target variables
     * @return unified answers
     */
    public static <T extends ReasonerQuery> QueryAnswers getUnifiedAnswers(T parentQuery, T childQuery, QueryAnswers answers){
        if (parentQuery == childQuery) return new QueryAnswers(answers);
        return answers.unify(childQuery.getUnifier(parentQuery));
    }
}
