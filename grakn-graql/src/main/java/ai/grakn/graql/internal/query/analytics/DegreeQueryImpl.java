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
import ai.grakn.concept.TypeName;
import ai.grakn.graql.analytics.DegreeQuery;
import ai.grakn.graql.internal.analytics.DegreeAndPersistVertexProgram;
import ai.grakn.graql.internal.analytics.DegreeDistributionMapReduce;
import ai.grakn.graql.internal.analytics.DegreeVertexProgram;
import ai.grakn.graql.internal.analytics.GraknMapReduce;
import ai.grakn.graql.internal.util.StringConverter;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static ai.grakn.graql.internal.analytics.CommonOLAP.analyticsElements;
import static ai.grakn.graql.internal.util.StringConverter.idToString;
import static java.util.stream.Collectors.joining;

class DegreeQueryImpl<T> extends AbstractComputeQuery<T> implements DegreeQuery<T> {

    private boolean persist = false;
    private boolean ofTypeNamesSet = false;
    private Set<TypeName> ofTypeNames = new HashSet<>();
    private TypeName degreeName = Schema.Analytics.DEGREE.getName();

    DegreeQueryImpl(Optional<GraknGraph> graph) {
        this.graph = graph;
    }

    @Override
    public T execute() {
        if (persist) LOGGER.info("DegreeAndPersistVertexProgram is called");
        else LOGGER.info("DegreeVertexProgram is called");
        initSubGraph();
        if (!selectedTypesHaveInstance()) return (T) Collections.emptyMap();
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

        if (persist) {
            if (!Sets.intersection(subTypeNames, analyticsElements).isEmpty()) {
                throw new IllegalStateException(ErrorMessage.ILLEGAL_ARGUMENT_EXCEPTION
                        .getMessage(this.getClass().toString()));
            }
            mutateResourceOntology(degreeName, ResourceType.DataType.LONG);
            waitOnMutateResourceOntology(degreeName);
            computer.compute(new DegreeAndPersistVertexProgram(withResourceRelationTypes, ofTypeNames,
                    keySpace, degreeName));

            LOGGER.info("DegreeAndPersistVertexProgram is done");
            return (T) "Degrees have been persisted";

        } else {
            result = computer.compute(new DegreeVertexProgram(withResourceRelationTypes, ofTypeNames),
                    new DegreeDistributionMapReduce(ofTypeNames));

            LOGGER.info("DegreeVertexProgram is done");
            return (T) result.memory().get(GraknMapReduce.MAP_REDUCE_MEMORY_KEY);
        }
    }

    @Override
    public boolean isReadOnly() {
        return !persist;
    }

    @Override
    public DegreeQuery<T> in(TypeName... subTypeNames) {
        return (DegreeQuery<T>) super.in(subTypeNames);
    }

    @Override
    public DegreeQuery<T> in(Collection<TypeName> subTypeNames) {
        return (DegreeQuery<T>) super.in(subTypeNames);
    }

    @Override
    public DegreeQuery<T> of(TypeName... ofTypeNames) {
        if (ofTypeNames.length > 0) {
            ofTypeNamesSet = true;
            this.ofTypeNames = Sets.newHashSet(ofTypeNames);
        }
        return this;
    }

    @Override
    public DegreeQuery<T> of(Collection<TypeName> ofTypeNames) {
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
            string += " of " + ofTypeNames.stream().map(type -> StringConverter.idToString(type.getValue())).collect(joining(", "));
        }

        string += subtypeString();

        if (persist) {
            string += " persist";
            if (!degreeName.equals(Schema.Analytics.DEGREE.getName())) {
                string += " " + idToString(degreeName.getValue());
            }
            string += ";";
        }

        return string;
    }

    @Override
    public DegreeQuery<T> withGraph(GraknGraph graph) {
        return (DegreeQuery<T>) super.withGraph(graph);
    }

}
