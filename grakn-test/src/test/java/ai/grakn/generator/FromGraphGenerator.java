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

package ai.grakn.generator;

import ai.grakn.GraknGraph;
import com.pholser.junit.quickcheck.generator.GeneratorConfiguration;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.function.Supplier;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE_USE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public abstract class FromGraphGenerator<T> extends AbstractGenerator<T> {
    private Supplier<GraknGraph> graphSupplier =
            () -> gen().make(GraknGraphs.class).setOpen(true).generate(random, status);

    private GraknGraph graph;

    FromGraphGenerator(Class<T> type) {
        super(type);
    }

    protected final GraknGraph graph() {
        return graph;
    }

    @Override
    protected final T generate() {
        graph = graphSupplier.get();
        return generateFromGraph();
    }

    protected abstract T generateFromGraph();

    protected final <S extends FromGraphGenerator<?>> S genFromGraph(Class<S> generatorClass) {
        S generator = gen().make(generatorClass);
        generator.fromGraph(this::graph);
        return generator;
    }

    public final void configure(FromGraph fromGraph) {
        fromLastGeneratedGraph();
    }

    final void fromGraph(Supplier<GraknGraph> graphSupplier) {
        this.graphSupplier = graphSupplier;
    }

    final FromGraphGenerator<T> fromLastGeneratedGraph() {
        fromGraph(GraknGraphs::lastGeneratedGraph);
        return this;
    }

    @Target({PARAMETER, FIELD, ANNOTATION_TYPE, TYPE_USE})
    @Retention(RUNTIME)
    @GeneratorConfiguration
    public @interface FromGraph {
    }
}
