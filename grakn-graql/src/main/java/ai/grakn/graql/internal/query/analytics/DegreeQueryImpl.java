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
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.analytics.DegreeQuery;
import ai.grakn.graql.internal.analytics.DegreeDistributionMapReduce;
import ai.grakn.graql.internal.analytics.DegreeVertexProgram;
import ai.grakn.graql.internal.util.StringConverter;
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
    private Set<Label> ofLabels = new HashSet<>();

    DegreeQueryImpl(Optional<GraknGraph> graph) {
        this.graph = graph;
    }

    @Override
    public Map<Long, Set<String>> execute() {
        LOGGER.info("DegreeVertexProgram is called");
        long startTime = System.currentTimeMillis();
        initSubGraph();

        // Check if ofType is valid before returning emptyMap
        if (ofLabels.isEmpty()) {
            ofLabels.addAll(subLabels);
        } else {
            ofLabels = ofLabels.stream()
                    .flatMap(typeLabel -> {
                        Type type = graph.get().getOntologyConcept(typeLabel);
                        if (type == null) throw GraqlQueryException.labelNotFound(typeLabel);
                        return type.subs().stream();
                    })
                    .map(OntologyConcept::getLabel)
                    .collect(Collectors.toSet());
            subLabels.addAll(ofLabels);
        }

        if (!selectedTypesHaveInstance()) return Collections.emptyMap();

        Set<Label> withResourceRelationTypes = getHasResourceRelationTypes();
        withResourceRelationTypes.addAll(subLabels);

        Set<LabelId> withResourceRelationLabelIds = convertLabelsToIds(withResourceRelationTypes);
        Set<LabelId> ofLabelIds = convertLabelsToIds(ofLabels);

        String randomId = getRandomJobId();

        ComputerResult result = getGraphComputer().compute(
                withResourceRelationLabelIds,
                new DegreeVertexProgram(ofLabelIds, randomId),
                new DegreeDistributionMapReduce(ofLabelIds, DegreeVertexProgram.DEGREE + randomId));

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
    public DegreeQuery in(Collection<Label> subLabels) {
        return (DegreeQuery) super.in(subLabels);
    }

    @Override
    public DegreeQuery of(String... ofTypeLabels) {
        if (ofTypeLabels.length > 0) {
            ofTypeLabelsSet = true;
            this.ofLabels = Arrays.stream(ofTypeLabels).map(Label::of).collect(Collectors.toSet());
        }
        return this;
    }

    @Override
    public DegreeQuery of(Collection<Label> ofLabels) {
        if (!ofLabels.isEmpty()) {
            ofTypeLabelsSet = true;
            this.ofLabels = Sets.newHashSet(ofLabels);
        }
        return this;
    }

    @Override
    String graqlString() {
        String string = "degrees";
        if (ofTypeLabelsSet) {
            string += " of " + ofLabels.stream()
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        DegreeQueryImpl that = (DegreeQueryImpl) o;

        return ofTypeLabelsSet == that.ofTypeLabelsSet && ofLabels.equals(that.ofLabels);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (ofTypeLabelsSet ? 1 : 0);
        result = 31 * result + ofLabels.hashCode();
        return result;
    }
}
