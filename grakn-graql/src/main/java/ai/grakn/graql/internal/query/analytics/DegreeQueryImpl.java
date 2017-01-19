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
import ai.grakn.concept.TypeName;
import ai.grakn.graql.analytics.DegreeQuery;
import ai.grakn.graql.internal.analytics.DegreeDistributionMapReduce;
import ai.grakn.graql.internal.analytics.DegreeVertexProgram;
import ai.grakn.graql.internal.util.StringConverter;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;

class DegreeQueryImpl extends AbstractComputeQuery<Map<Long, Set<String>>> implements DegreeQuery {

    private boolean ofTypeNamesSet = false;
    private Set<TypeName> ofTypeNames = new HashSet<>();

    DegreeQueryImpl(Optional<GraknGraph> graph) {
        this.graph = graph;
    }

    @Override
    public Map<Long, Set<String>> execute() {
        LOGGER.info("DegreeVertexProgram is called");
        initSubGraph();
        if (!selectedTypesHaveInstance()) return Collections.emptyMap();
        ofTypeNames.forEach(type -> {
            if (!subTypeNames.contains(type)) {
                throw new IllegalStateException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION
                        .getMessage(type));
            }
        });

        ComputerResult result;
        GraknComputer computer = getGraphComputer();

        Set<TypeName> withResourceRelationTypes = getHasResourceRelationTypes();
        withResourceRelationTypes.addAll(subTypeNames);

        if (ofTypeNames.isEmpty()) {
            ofTypeNames.addAll(subTypeNames);
        }

        result = computer.compute(new DegreeVertexProgram(withResourceRelationTypes, ofTypeNames),
                new DegreeDistributionMapReduce(ofTypeNames));

        LOGGER.info("DegreeVertexProgram is done");
        return result.memory().get(DegreeDistributionMapReduce.class.getName());
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public DegreeQuery in(String... subTypeNames) {
        return (DegreeQuery) super.in(subTypeNames);
    }

    @Override
    public DegreeQuery in(Collection<TypeName> subTypeNames) {
        return (DegreeQuery) super.in(subTypeNames);
    }

    @Override
    public DegreeQuery of(String... ofTypeNames) {
        if (ofTypeNames.length > 0) {
            ofTypeNamesSet = true;
            this.ofTypeNames = Arrays.stream(ofTypeNames).map(TypeName::of).collect(Collectors.toSet());
        }
        return this;
    }

    @Override
    public DegreeQuery of(Collection<TypeName> ofTypeNames) {
        if (!ofTypeNames.isEmpty()) {
            ofTypeNamesSet = true;
            this.ofTypeNames = Sets.newHashSet(ofTypeNames);
        }
        return this;
    }

    @Override
    String graqlString() {
        String string = "degrees";
        if (ofTypeNamesSet) {
            string += " of " + ofTypeNames.stream()
                    .map(StringConverter::typeNameToString)
                    .collect(joining(", "));
        }
        string += subtypeString();
        return string;
    }

    @Override
    public DegreeQuery withGraph(GraknGraph graph) {
        return (DegreeQuery) super.withGraph(graph);
    }
}
