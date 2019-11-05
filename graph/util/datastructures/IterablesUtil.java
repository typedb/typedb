/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graph.util.datastructures;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Utility class for interacting with {@link Iterable}.
 *
 */
public class IterablesUtil {

    public static <O> Iterable<O> emptyIterable() {
        return Collections::emptyIterator;
    }

    public static final Predicate NO_FILTER = new NoFilter();

    public static <E> Predicate<E> noFilter() {
        return (Predicate<E>)NO_FILTER;
    }

    private static class NoFilter<E> implements Predicate<E> {

        @Override
        public boolean apply(@Nullable E e) {
            return true;
        }
    }

    public static <O> Iterable<O> limitedIterable(Iterable<O> iterable, int limit) {
        return StreamSupport.stream(iterable.spliterator(), false).limit(limit).collect(Collectors.toList());
    }

    public static int size(Iterable i) {
        if (i instanceof Collection) return ((Collection)i).size();
        else return Iterables.size(i);
    }

    public static boolean sizeLargerOrEqualThan(Iterable i, int limit) {
        if (i instanceof Collection) return ((Collection)i).size()>=limit;
        Iterator iterator = i.iterator();
        int count=0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
            if (count>=limit) return true;
        }
        return false;
    }

    public static<E> List<E> mergeSort(Collection<E> a, Collection<E> b, Comparator<E> comp) {
        Iterator<E> iteratorA = a.iterator(), iteratorB = b.iterator();
        E headA = iteratorA.hasNext()?iteratorA.next():null;
        E headB = iteratorB.hasNext()?iteratorB.next():null;
        List<E> result = new ArrayList<>(a.size()+b.size());
        while (headA!=null || headB!=null) {
            E next;
            if (headA==null) {
                next=headB;
                headB = null;
            } else if (headB==null) {
                next=headA;
                headA=null;
            } else if (comp.compare(headA,headB)<=0) {
                next=headA;
                headA=null;
            } else {
                next=headB;
                headB=null;
            }
            Preconditions.checkArgument(result.isEmpty() || comp.compare(result.get(result.size()-1),next)<=0,
                    "The input collections are not sorted");
            result.add(next);
            if (headA==null) headA=iteratorA.hasNext()?iteratorA.next():null;
            if (headB==null) headB=iteratorB.hasNext()?iteratorB.next():null;
        }
        return result;
    }



}
