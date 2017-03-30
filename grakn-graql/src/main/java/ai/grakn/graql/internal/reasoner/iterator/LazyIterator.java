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

package ai.grakn.graql.internal.reasoner.iterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 *
 * <p>
 * Lazy iterator class allowing for rewinding streams by accumulating consumed results.
 * </p>
 *
 * @param <T> the type of element that this iterator will iterate over
 *
 * @author Kasper Piskorski
 *
 */
public class LazyIterator<T> implements Iterable<T>{
    private final Iterator<T> iterator;
    private final List<T> accumulator = new ArrayList<>();

    public LazyIterator(){iterator = Collections.emptyIterator();}
    public LazyIterator(Stream<T> stream){
        this.iterator = stream.distinct().iterator();
    }
    public LazyIterator(Iterator<T> iterator){ this.iterator = iterator;}

    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>(){
            int index = 0;
            @Override
            public boolean hasNext() {
                return index < accumulator.size() || iterator.hasNext();
            }

            @Override
            public T next() {
                if (index >= accumulator.size()){
                    T elem = iterator.next();
                    accumulator.add(elem);
                }
                T elem = accumulator.get(index);
                index++;
                return elem;
            }
        };
    }

    public LazyIterator<T> merge(Stream<T> stream){
        return new LazyIterator<>(Stream.concat(this.stream(), stream));
    }

    public Stream<T> stream() {
        return StreamSupport.stream(this.spliterator(), false).distinct();
    }
    public long size(){ return accumulator.size();}
}
