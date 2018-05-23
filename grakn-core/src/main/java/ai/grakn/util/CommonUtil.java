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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.util;

import ai.grakn.GraknSystemProperty;
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Common utility methods used within Grakn.
 *
 * Some of these methods are Grakn-specific, others add important "missing" methods to Java/Guava classes.
 *
 * @author Felix Chapman
 */
public class CommonUtil {

    private CommonUtil() {}

    /**
     * @return The project path. If it is not specified as a JVM parameter it will be set equal to
     * user.dir folder.
     */
    public static Path getProjectPath() {
        if (GraknSystemProperty.CURRENT_DIRECTORY.value() == null) {
            GraknSystemProperty.CURRENT_DIRECTORY.set(StandardSystemProperty.USER_DIR.value());
        }
        return Paths.get(GraknSystemProperty.CURRENT_DIRECTORY.value());
    }

    /**
     * @param optional the optional to change into a output
     * @param <T> the type in the optional
     * @return a output of one item if the optional has an element, else an empty output
     */
    public static <T> Stream<T> optionalToStream(Optional<T> optional) {
        return optional.map(Stream::of).orElseGet(Stream::empty);
    }

    public static <T> Stream<T> stream(Iterator<T> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
    }

    @SafeVarargs
    public static <T> Optional<T> optionalOr(Optional<T>... options) {
        return Stream.of(options).flatMap(CommonUtil::optionalToStream).findFirst();
    }

    /**
     * Helper which lazily checks if a {@link Stream} contains the number specified
     * WARNING: This consumes the output rendering it unusable afterwards
     *
     * @param stream the {@link Stream} to check the count against
     * @param size the expected number of elements in the output
     * @return true if the expected size is found
     */
    public static boolean containsOnly(Stream stream, long size){
        long count = 0L;
        Iterator it = stream.iterator();

        while(it.hasNext()){
            it.next();
            if(++count > size) return false;
        }

        return size == count;
    }

    @CheckReturnValue
    public static RuntimeException unreachableStatement(Throwable cause) {
        return unreachableStatement(null, cause);
    }

    @CheckReturnValue
    public static RuntimeException unreachableStatement(String message) {
        return unreachableStatement(message, null);
    }

    @CheckReturnValue
    public static RuntimeException unreachableStatement(@Nullable String message, Throwable cause) {
        return new RuntimeException("Statement expected to be unreachable: " + message, cause);
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

    public static StringBuilder simplifyExceptionMessage(Throwable e) {
        StringBuilder message = new StringBuilder(e.getMessage());
        while(e.getCause() != null) {
            e = e.getCause();
            message.append("\n").append(e.getMessage());
        }
        return message;
    }
}
