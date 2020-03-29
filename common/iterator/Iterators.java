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

package hypergraph.common.iterator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public class Iterators {

    public static <T> LoopIterator<T> loop(T seed, Predicate<T> predicate, UnaryOperator<T> function) {
        return new LoopIterator<>(seed, predicate, function);
    }

    @SafeVarargs
    public static <T> LinkedIterators<T> link(Iterator<T>... iterators) {
        return new LinkedIterators<>(new LinkedList<>(Arrays.asList(iterators)));
    }

    public static <T> FilteredIterator<T> filter(Iterator<T> iterator, Predicate<T> predicate) {
        return new FilteredIterator<>(iterator, predicate);
    }

    public static <T, U> AppliedIterator<T, U> apply(Iterator<T> iterator, Function<T, U> function) {
        return new AppliedIterator<>(iterator, function);
    }

    public interface Composable<T> extends Iterator<T> {

        default LinkedIterators<T> link(Iterator<T> iterator) {
            return new LinkedIterators<>(new LinkedList<>(Arrays.asList(this, iterator)));
        }

        default FilteredIterator<T> filter(Predicate<T> predicate) {
            return new FilteredIterator<>(this, predicate);
        }

        default <U> AppliedIterator<T, U> apply(Function<T, U> function) {
            return new AppliedIterator<>(this, function);
        }
    }
}
