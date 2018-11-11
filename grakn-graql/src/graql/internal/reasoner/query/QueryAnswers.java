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

package grakn.core.graql.internal.reasoner.query;

import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.admin.MultiUnifier;
import grakn.core.graql.admin.Unifier;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/**
 *
 * <p>
 * Wrapper class for a set of {@link ConceptMap} objects providing higher level facilities.
 *
 *
 */
public class QueryAnswers implements Iterable<ConceptMap>{

    private final HashSet<ConceptMap> set = new HashSet<>();

    @Nonnull
    @Override
    public Iterator<ConceptMap> iterator() { return set.iterator();}

    @Override
    public boolean equals(Object obj){
        if (obj == this) return true;
        if (obj == null || !(obj instanceof QueryAnswers)) return false;
        QueryAnswers a2 = (QueryAnswers) obj;
        return set.equals(a2.set);
    }

    @Override
    public int hashCode(){return set.hashCode();}

    @Override
    public String toString(){ return set.toString();}

    public Stream<ConceptMap> stream(){ return set.stream();}

    public QueryAnswers(){}
    public QueryAnswers(ConceptMap ans){ set.add(ans);}
    public QueryAnswers(Collection<ConceptMap> ans){ set.addAll(ans); }
    private QueryAnswers(QueryAnswers ans){ ans.forEach(set::add);}

    public boolean add(ConceptMap a){ return set.add(a);}
    public boolean addAll(QueryAnswers ans){ return set.addAll(ans.set);}

    public boolean removeAll(QueryAnswers ans){ return set.removeAll(ans.set);}

    public boolean contains(ConceptMap a){ return set.contains(a);}
    public boolean isEmpty(){ return set.isEmpty();}

    /**
     * unify the answers by applying unifier to variable set
     * @param unifier map of [key: from/value: to] unifiers
     * @return unified query answers
     */
    public QueryAnswers unify(Unifier unifier){
        if (unifier.isEmpty()) return new QueryAnswers(this);
        QueryAnswers unifiedAnswers = new QueryAnswers();
        this.stream()
            .map(a -> a.unify(unifier))
            .filter(a -> !a.isEmpty())
            .forEach(unifiedAnswers::add);
        return unifiedAnswers;
    }

    /**
     * unify the answers by applying multiunifier to variable set
     * @param multiUnifier multiunifier to be applied to the query answers
     * @return unified query answers
     */
    public QueryAnswers unify(MultiUnifier multiUnifier){
        QueryAnswers unifiedAnswers = new QueryAnswers();
        this.stream()
                .flatMap(a -> a.unify(multiUnifier))
                .filter(a -> !a.isEmpty())
                .forEach(unifiedAnswers::add);
        return unifiedAnswers;
    }

}
