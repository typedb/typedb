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

import ai.grakn.GraknGraph;
import ai.grakn.concept.TypeLabel;
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

    private boolean ofTypeLabelsSet = false;
    private Set<TypeLabel> ofTypeLabels = new HashSet<>();

    DegreeQueryImpl(Optional<GraknGraph> graph) {
        this.graph = graph;
    }

    @Override
    public Map<Long, Set<String>> execute() {
        LOGGER.info("DegreeVertexProgram is called");
        long startTime = System.currentTimeMillis();
        initSubGraph();
        if (!selectedTypesHaveInstance()) return Collections.emptyMap();
        ofTypeLabels.forEach(type -> {
            if (!subTypeLabels.contains(type)) {
                throw new IllegalStateException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION
                        .getMessage(type));
            }
        });

        ComputerResult result;

        Set<TypeLabel> withResourceRelationTypes = getHasResourceRelationTypes();
        withResourceRelationTypes.addAll(subTypeLabels);

        if (ofTypeLabels.isEmpty()) {
            ofTypeLabels.addAll(subTypeLabels);
        }

        result = getGraphComputer().compute(new DegreeVertexProgram(withResourceRelationTypes, ofTypeLabels),
                new DegreeDistributionMapReduce(ofTypeLabels));

        LOGGER.info("DegreeVertexProgram is done in " + (System.currentTimeMillis() - startTime) + " ms");
        return result.memory().get(DegreeDistributionMapReduce.class.getName());
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public DegreeQuery in(String... subTypeLabels) {
        return (DegreeQuery) super.in(subTypeLabels);
    }

    @Override
    public DegreeQuery in(Collection<TypeLabel> subTypeLabels) {
        return (DegreeQuery) super.in(subTypeLabels);
    }

    @Override
    public DegreeQuery of(String... ofTypeLabels) {
        if (ofTypeLabels.length > 0) {
            ofTypeLabelsSet = true;
            this.ofTypeLabels = Arrays.stream(ofTypeLabels).map(TypeLabel::of).collect(Collectors.toSet());
        }
        return this;
    }

    @Override
    public DegreeQuery of(Collection<TypeLabel> ofTypeLabels) {
        if (!ofTypeLabels.isEmpty()) {
            ofTypeLabelsSet = true;
            this.ofTypeLabels = Sets.newHashSet(ofTypeLabels);
        }
        return this;
    }

    @Override
    String graqlString() {
        String string = "degrees";
        if (ofTypeLabelsSet) {
            string += " of " + ofTypeLabels.stream()
                    .map(StringConverter::typeLabelToString)
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
