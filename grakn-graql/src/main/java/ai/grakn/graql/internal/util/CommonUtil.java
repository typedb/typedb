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

package ai.grakn.graql.internal.util;

import ai.grakn.concept.Concept;
import ai.grakn.graql.VarName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

/**
 * Common utility methods used within Graql.
 *
 * Some of these methods are Graql-specific, others add important "missing" methods to Java/Guava classes.
 *
 * @author Felix Chapman
 */
public class CommonUtil {

    private CommonUtil() {}

    /**
     * @param optional the optional to change into a stream
     * @param <T> the type in the optional
     * @return a stream of one item if the optional has an element, else an empty stream
     */
    public static <T> Stream<T> optionalToStream(Optional<T> optional) {
        return optional.map(Stream::of).orElseGet(Stream::empty);
    }

    public static <T> Optional<T> tryNext(Iterator<T> iterator) {
        if (iterator.hasNext()) {
            return Optional.of(iterator.next());
        } else {
            return Optional.empty();
        }
    }

    public static <T> Optional<T> tryAny(Iterable<T> iterable) {
        return tryNext(iterable.iterator());
    }

    @SafeVarargs
    public static <T> Optional<T> optionalOr(Optional<T>... options) {
        return Stream.of(options).flatMap(CommonUtil::optionalToStream).findFirst();
    }

    public static Map<String, Concept> resultVarNameToString(Map<VarName, Concept> result) {
        return result.entrySet().stream().collect(toMap(entry -> entry.getKey().getValue(), Map.Entry::getValue));
    }

    public static <T> Collector<T, ?, ImmutableSet<T>> toImmutableSet() {
        return new Collector<T, ImmutableSet.Builder<T>, ImmutableSet<T>>() {
            @Override
            public Supplier<ImmutableSet.Builder<T>> supplier() {
                return ImmutableSet::builder;
            }

            @Override
            public BiConsumer<ImmutableSet.Builder<T>, T> accumulator() {
                return ImmutableSet.Builder::add;
            }

            @Override
            public BinaryOperator<ImmutableSet.Builder<T>> combiner() {
                return (b1, b2) -> b1.addAll(b2.build());
            }

            @Override
            public Function<ImmutableSet.Builder<T>, ImmutableSet<T>> finisher() {
                return ImmutableSet.Builder::build;
            }

            @Override
            public Set<Characteristics> characteristics() {
                return ImmutableSet.of();
            }
        };
    }

    public static <T> Collector<T, ?, ImmutableList<T>> toImmutableList() {
        return new Collector<T, ImmutableList.Builder<T>, ImmutableList<T>>() {
            @Override
            public Supplier<ImmutableList.Builder<T>> supplier() {
                return ImmutableList::builder;
            }

            @Override
            public BiConsumer<ImmutableList.Builder<T>, T> accumulator() {
                return ImmutableList.Builder::add;
            }

            @Override
            public BinaryOperator<ImmutableList.Builder<T>> combiner() {
                return (b1, b2) -> b1.addAll(b2.build());
            }

            @Override
            public Function<ImmutableList.Builder<T>, ImmutableList<T>> finisher() {
                return ImmutableList.Builder::build;
            }

            @Override
            public Set<Characteristics> characteristics() {
                return ImmutableSet.of();
            }
        };
    }

    public static <T> Collector<T, ?, ImmutableMultiset<T>> toImmutableMultiset() {
        return new Collector<T, ImmutableMultiset.Builder<T>, ImmutableMultiset<T>>() {
            @Override
            public Supplier<ImmutableMultiset.Builder<T>> supplier() {
                return ImmutableMultiset::builder;
            }

            @Override
            public BiConsumer<ImmutableMultiset.Builder<T>, T> accumulator() {
                return ImmutableMultiset.Builder::add;
            }

            @Override
            public BinaryOperator<ImmutableMultiset.Builder<T>> combiner() {
                return (b1, b2) -> b1.addAll(b2.build());
            }

            @Override
            public Function<ImmutableMultiset.Builder<T>, ImmutableMultiset<T>> finisher() {
                return ImmutableMultiset.Builder::build;
            }

            @Override
            public Set<Characteristics> characteristics() {
                return ImmutableSet.of();
            }
        };
    }
}
