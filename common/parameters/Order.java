/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.parameters;

import java.util.Iterator;
import java.util.NavigableSet;

public abstract class Order {

    public abstract Orderer orderer();

    public <T extends Comparable<? super T>> boolean inOrder(T first, T second) {
        return orderer().compare(first, second) <= 0;
    }

    public <T extends Comparable<? super T>> boolean inOrderNotEq(T first, T second) {
        return orderer().compare(first, second) < 0;
    }

    public boolean isAscending() {
        return false;
    }

    public boolean isDescending() {
        return false;
    }

    public interface Orderer {

        <T extends Comparable<? super T>> int compare(T last, T next);

        <T extends Comparable<? super T>> Iterator<T> iterate(NavigableSet<T> source);

        <T extends Comparable<? super T>> Iterator<T> iterate(NavigableSet<T> source, T from);
    }

    public static class Asc extends Order {

        public static final Asc ASC = new Asc();

        private static final Orderer orderer = new Orderer() {
            @Override
            public <T extends Comparable<? super T>> int compare(T last, T next) {
                return last.compareTo(next);
            }

            @Override
            public <T extends Comparable<? super T>> Iterator<T> iterate(NavigableSet<T> source) {
                return source.iterator();
            }

            @Override
            public <T extends Comparable<? super T>> Iterator<T> iterate(NavigableSet<T> source, T from) {
                return source.tailSet(from, true).iterator();
            }
        };

        @Override
        public Orderer orderer() {
            return orderer;
        }

        @Override
        public boolean isAscending() {
            return true;
        }
    }

    public static class Desc extends Order {

        public static final Desc DESC = new Desc();

        private static final Orderer orderer = new Orderer() {

            @Override
            public <T extends Comparable<? super T>> int compare(T last, T next) {
                return -1 * last.compareTo(next);
            }

            @Override
            public <T extends Comparable<? super T>> Iterator<T> iterate(NavigableSet<T> source) {
                return source.descendingIterator();
            }

            @Override
            public <T extends Comparable<? super T>> Iterator<T> iterate(NavigableSet<T> source, T from) {
                return source.headSet(from, true).descendingIterator();
            }
        };

        @Override
        public Orderer orderer() {
            return orderer;
        }

        @Override
        public boolean isDescending() {
            return true;
        }
    }
}
