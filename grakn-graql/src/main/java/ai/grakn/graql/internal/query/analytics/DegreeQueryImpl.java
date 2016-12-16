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

package ai.grakn.graql.internal.query.analytics;

import ai.grakn.GraknComputer;
import ai.grakn.GraknGraph;
import ai.grakn.concept.ResourceType;
import ai.grakn.graql.analytics.DegreeQuery;
import ai.grakn.graql.internal.analytics.DegreeAndPersistVertexProgram;
import ai.grakn.graql.internal.analytics.DegreeDistributionMapReduce;
import ai.grakn.graql.internal.analytics.DegreeVertexProgram;
import ai.grakn.graql.internal.analytics.GraknMapReduce;
import ai.grakn.graql.internal.util.StringConverter;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static ai.grakn.graql.internal.analytics.CommonOLAP.analyticsElements;
import static java.util.stream.Collectors.joining;

public class DegreeQueryImpl<T> extends AbstractComputeQuery<T> implements DegreeQuery<T> {

    private boolean persist = false;
    private Set<String> ofTypeNames = new HashSet<>();

    public DegreeQueryImpl(Optional<GraknGraph> graph) {
        this.graph = graph;
    }

    @Override
    public T execute() {
        if (persist) LOGGER.info("DegreeAndPersistVertexProgram is called");
        else LOGGER.info("DegreeVertexProgram is called");
        initSubGraph();
        if (!selectedTypesHaveInstance()) return (T) Collections.emptyMap();
        ofTypeNames.forEach(type -> {
            if (!subTypeNames.contains(type))
                throw new IllegalStateException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION
                        .getMessage(type));
        });

        ComputerResult result;
        GraknComputer computer = getGraphComputer();

        if (persist) {
            if (!Sets.intersection(subTypeNames, analyticsElements).isEmpty()) {
                throw new IllegalStateException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION
                        .getMessage(this.getClass().toString()));
            }
            mutateResourceOntology(degree, ResourceType.DataType.LONG);
            waitOnMutateResourceOntology(degree);
            computer.compute(new DegreeAndPersistVertexProgram(subTypeNames, keySpace, ofTypeNames));
            LOGGER.info("DegreeAndPersistVertexProgram is done");
            return (T) "Degrees have been persisted";
        } else {
            if (ofTypeNames.isEmpty()) {
                result = computer.compute(new DegreeVertexProgram(subTypeNames, ofTypeNames),
                        new DegreeDistributionMapReduce(subTypeNames));
            } else {
                result = computer.compute(new DegreeVertexProgram(subTypeNames, ofTypeNames),
                        new DegreeDistributionMapReduce(ofTypeNames));
            }
            LOGGER.info("DegreeVertexProgram is done");
            return (T) result.memory().get(GraknMapReduce.MAP_REDUCE_MEMORY_KEY);
        }
    }

    @Override
    public boolean isReadOnly() {
        return !persist;
    }

    @Override
    public DegreeQuery<String> persist() {
        this.persist = true;
        return (DegreeQuery<String>) this;
    }

    @Override
    public DegreeQuery<T> in(String... subTypeNames) {
        return (DegreeQuery<T>) super.in(subTypeNames);
    }

    @Override
    public DegreeQuery<T> in(Collection<String> subTypeNames) {
        return (DegreeQuery<T>) super.in(subTypeNames);
    }

    @Override
    public DegreeQuery<T> of(String... ofTypeNames) {
        this.ofTypeNames = Sets.newHashSet(ofTypeNames);
        return this;
    }

    @Override
    public DegreeQuery<T> of(Collection<String> ofTypeNames) {
        this.ofTypeNames = Sets.newHashSet(ofTypeNames);
        return this;
    }

    @Override
    String graqlString() {
        String string = "degrees" + subtypeString();

        if (!ofTypeNames.isEmpty()) string += " of " + ofTypeNames.stream()
                .map(StringConverter::idToString).collect(joining(", ")) + ";";

        if (persist) string += " persist;";

        return string;
    }

    @Override
    public DegreeQuery<T> withGraph(GraknGraph graph) {
        return (DegreeQuery<T>) super.withGraph(graph);
    }

}
