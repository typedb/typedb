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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.query;

/*-
 * #%L
 * grakn-graql
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.admin.Unifier;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Wrapper class for a set of {@link Answer} objects providing higher level facilities.
 *
 * @author Kasper Piskorski
 *
 */
public class QueryAnswers implements Iterable<Answer>{

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
    public QueryAnswers(Collection<Answer> ans){ set.addAll(ans); }
    public QueryAnswers(QueryAnswers ans){ ans.forEach(set::add);}

    public boolean add(Answer a){ return set.add(a);}
    public boolean addAll(QueryAnswers ans){ return set.addAll(ans.set);}

    public boolean removeAll(QueryAnswers ans){ return set.removeAll(ans.set);}

    public boolean contains(Answer a){ return set.contains(a);}

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
