/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.iterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

class PermutationIterator<T> extends AbstractFunctionalIterator<List<T>> {

    private final List<T> list;
    final int[] visitCounters;
    final int[] directions;
    private State state;

    PermutationIterator(Collection<T> list) {
        this.list = new ArrayList<>(list);
        int n = list.size();
        visitCounters = new int[n];
        directions = new int[n];
        Arrays.fill(visitCounters, 0);
        Arrays.fill(directions, 1);
        state = State.FETCHED;
    }

    private enum State {EMPTY, FETCHED, COMPLETED} // TODO: all needed?

    @Override
    public boolean hasNext() {
        switch (state) {
            case EMPTY:
                return fetchAndCheck();
            case FETCHED:
                return true;
            case COMPLETED:
                return false;
            default: // This should never be reached
                return false;
        }
    }

    private boolean fetchAndCheck() {
        state = State.FETCHED;
        calculateNextPermutation();
        return state == State.FETCHED;
    }

    @Override
    public List<T> next() {
        if (!hasNext()) throw new NoSuchElementException();
        state = State.EMPTY;
        return Collections.unmodifiableList(list);
    }

    void calculateNextPermutation() {
        int pointer = list.size() - 1;
        int switchDirOffset = 0;

        if (pointer == -1) {
            // If list is empty
            state = State.COMPLETED;
            return;
        }

        while (true) {
            int swapOffset = visitCounters[pointer] + directions[pointer];
            if (swapOffset < 0) {
                pointer = switchDirection(pointer);
                continue;
            }
            if (swapOffset == pointer + 1) {
                if (pointer == 0) {
                    state = State.COMPLETED;
                    break;
                }
                switchDirOffset++;
                pointer = switchDirection(pointer);
                continue;
            }
            // TODO: identify the proper names for 'i' and 'j'
            int i = pointer - visitCounters[pointer] + switchDirOffset;
            int j = pointer - swapOffset + switchDirOffset;
            Collections.swap(list, i, j);
            visitCounters[pointer] = swapOffset;
            break;
        }
    }

    int switchDirection(int pointer) {
        directions[pointer] = -directions[pointer];
        return pointer - 1;
    }

    @Override
    public void recycle() {}
}
