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

package ai.grakn.graql.internal.analytics;

import ai.grakn.concept.LabelId;
import ai.grakn.concept.ResourceType;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Set;

import static ai.grakn.graql.internal.analytics.Utility.vertexHasSelectedTypeId;

/**
 * The abstract MapReduce program for computing statistics.
 * <p>
 *
 * @author Jason Liu
 */

abstract class StatisticsMapReduce<T> extends GraknMapReduce<T> {

    String degreePropertyKey;

    StatisticsMapReduce() {

    }

    StatisticsMapReduce(Set<LabelId> selectedLabelIds, ResourceType.DataType resourceDataType, String degreePropertyKey) {
        super(selectedLabelIds, resourceDataType);
        this.degreePropertyKey = degreePropertyKey;
        this.persistentProperties.put(DegreeVertexProgram.DEGREE, degreePropertyKey);
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        super.loadState(graph, configuration);
        degreePropertyKey = (String) persistentProperties.get(DegreeVertexProgram.DEGREE);
    }

    boolean resourceIsValid(Vertex vertex) {
        return vertexHasSelectedTypeId(vertex, selectedTypes) && vertex.<Long>value(degreePropertyKey) > 0;
    }
}
