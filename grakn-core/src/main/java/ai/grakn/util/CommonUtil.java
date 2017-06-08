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
 *
 */

package ai.grakn.util;

import ai.grakn.GraknGraph;
import com.google.common.collect.Streams;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Common utility methods used within Grakn.
 *
 * Some of these methods are Grakn-specific, others add important "missing" methods to Java/Guava classes.
 *
 * @author Felix Chapman
 */
public class CommonUtil {

    private CommonUtil() {}

    public static void withImplicitConceptsVisible(GraknGraph graph, Runnable function) {
        withImplicitConceptsVisible(graph, g -> {
            function.run();
            return null;
        });
    }

    public static <T> T withImplicitConceptsVisible(GraknGraph graph, Supplier<T> function) {
        return withImplicitConceptsVisible(graph, g -> function.get());
    }

    public static <T> T withImplicitConceptsVisible(GraknGraph graph, Function<GraknGraph, T> function) {
        boolean implicitFlag = graph.implicitConceptsVisible();
        graph.showImplicitConcepts(true);
        T result;
        try {
            result = function.apply(graph);
        } finally {
            graph.showImplicitConcepts(implicitFlag);
        }
        return result;
    }

    @SafeVarargs
    public static <T> Optional<T> optionalOr(Optional<T>... options) {
        return Stream.of(options).flatMap(Streams::stream).findFirst();
    }
}
