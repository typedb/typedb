/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.common.iterator.sorted;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class IntersectForwardableIterator<T extends Comparable<? super T>, ORDER extends SortedIterator.Order>
        extends AbstractSortedIterator<T, ORDER>
        implements SortedIterator.Forwardable<T, ORDER> {

    private final List<Forwardable<T, ORDER>> iterators;

    private T candidate;
    private int candidateSource;
    private State state;

    private enum State {INIT, EMPTY, FETCHED, COMPLETED}

    IntersectForwardableIterator(List<Forwardable<T, ORDER>> iterators, ORDER order) {
        super(order);
        this.iterators = iterators;
        state = State.INIT;
    }

    @Override
    public boolean hasNext() {
        switch (state) {
            case INIT:
                return fetch();
            case EMPTY:
                iterators.forEach(Iterator::next);
                return fetch();
            case FETCHED:
                return true;
            case COMPLETED:
                return false;
            default:
                throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    private boolean fetch() {
        Forwardable<T, ORDER> iterator = iterators.get(0);
        if (!iterator.hasNext()) state = State.COMPLETED;
        else {
            candidateSource = 0;
            candidate = iterator.peek();
            while (state != State.COMPLETED && state != State.FETCHED) {
                verifyOrProposeCandidate();
            }
        }
        return state == State.FETCHED;
    }

    /**
     * To make the intersection more efficient, we continue to scan all iterators to find the next best candidate
     * even if the existing one is not matched quickly.
     * This guarantees we pay at most `k*min(N_1, N_2, ... N_k)` where `k` is the number of iterators
     * and `N_i` is the size of an iterator.
     */
    private void verifyOrProposeCandidate() {
        boolean newCandidate = false;
        for (int i = 0; i < iterators.size(); i++) {
            if (i == candidateSource) continue;
            Forwardable<T, ORDER> iterator = iterators.get(i);
            if (iterator.hasNext() && !iterator.peek().equals(candidate)) iterator.forward(candidate);
            if (!iterator.hasNext()) {
                state = State.COMPLETED;
                return;
            } else if (!iterator.peek().equals(candidate)) {
                assert order.isValidNext(candidate, iterator.peek());
                candidate = iterator.peek();
                candidateSource = i;
                newCandidate = true;
            }
        }
        if (!newCandidate) state = State.FETCHED;
    }

    @Override
    public T peek() {
        if (!hasNext()) throw new NoSuchElementException();
        return candidate;
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        state = State.EMPTY;
        return candidate;
    }

    @Override
    public void forward(T target) {
        iterators.forEach(iterator -> iterator.forward(target));
        state = State.INIT;
    }

    @Override
    public void recycle() {
        iterators.forEach(FunctionalIterator::recycle);
    }

    @Override
    public final SortedIterator.Forwardable<T, ORDER> merge(SortedIterator.Forwardable<T, ORDER> iterator) {
        return SortedIterators.Forwardable.merge(this, iterator);
    }

    @Override
    public final SortedIterator.Forwardable<T, ORDER> intersect(SortedIterator.Forwardable<T, ORDER> iterator) {
        return SortedIterators.Forwardable.intersect(this, iterator);
    }

    @Override
    public <U extends Comparable<? super U>, ORD extends Order> SortedIterator.Forwardable<U, ORD> mapSorted(
            Function<T, U> mappingFn, Function<U, T> reverseMappingFn, ORD order) {
        return SortedIterators.Forwardable.mapSorted(order, this, mappingFn, reverseMappingFn);
    }

    @Override
    public SortedIterator.Forwardable<T, ORDER> distinct() {
        return SortedIterators.Forwardable.distinct(this);
    }

    @Override
    public SortedIterator.Forwardable<T, ORDER> filter(Predicate<T> predicate) {
        return SortedIterators.Forwardable.filter(this, predicate);
    }

    @Override
    public SortedIterator.Forwardable<T, ORDER> limit(long limit) {
        return SortedIterators.Forwardable.limit(this, limit);
    }

    @Override
    public SortedIterator.Forwardable<T, ORDER> onConsumed(Runnable function) {
        return SortedIterators.Forwardable.onConsume(this, function);
    }

    @Override
    public SortedIterator.Forwardable<T, ORDER> onFinalise(Runnable function) {
        return SortedIterators.Forwardable.onFinalise(this, function);
    }
}
