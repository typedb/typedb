/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.graql.reasoner.plan;

import com.google.common.base.Equivalence;
import grakn.core.graql.reasoner.query.ReasonerQueryImpl;

import java.util.Collection;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Helper class for collections of ReasonerQueryImpl queries with equality comparison ReasonerQueryEquivalence.
 * </p>
 *
 * @param <T> unwrapped collection type
 * @param <W> wrapped collection type
 *
 *
 */
public abstract class QueryCollection<T extends Collection<ReasonerQueryImpl>, W extends Collection<Equivalence.Wrapper<ReasonerQueryImpl>>> extends QueryCollectionBase{

    T collection;
    W wrappedCollection;

    @Override
    public Stream<Equivalence.Wrapper<ReasonerQueryImpl>> wrappedStream(){ return wrappedCollection.stream(); }

    @Override
    public Stream<ReasonerQueryImpl> stream() { return collection.stream(); }

    @Override
    public String toString(){ return collection.toString();}

    public T toCollection(){ return collection;}

    public W toWrappedCollection(){ return wrappedCollection;}

    public boolean contains(ReasonerQueryImpl q){
        return this.contains(equality().wrap(q));
    }

    public boolean contains(Equivalence.Wrapper<ReasonerQueryImpl> q){
        return wrappedCollection.contains(q);
    }

    public boolean containsAll(QueryCollection<T, W> queries){
        return queries.wrappedStream().allMatch(this::contains);
    }

    public boolean add(ReasonerQueryImpl q){
        return collection.add(q) && wrappedCollection.add(equality().wrap(q));
    }

    public boolean add(Equivalence.Wrapper<ReasonerQueryImpl> q){
        return collection.add(q.get()) && wrappedCollection.add(q);
    }

    public int size(){ return collection.size();}

    public boolean isEmpty(){ return collection.isEmpty() && wrappedCollection.isEmpty();}

}
