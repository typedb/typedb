/*
 * Copyright (C) 2022 Vaticle
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
