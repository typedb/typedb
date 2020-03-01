/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.analytics;

import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.LabelId;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Set;

import static grakn.core.graql.analytics.Utility.vertexHasSelectedTypeId;

/**
 * The abstract MapReduce program for computing statistics.
 *
 * @param <T> Determines the return type of th MapReduce statistics computation
 */
public abstract class StatisticsMapReduce<T> extends GraknMapReduce<T> {

    String degreePropertyKey;

    StatisticsMapReduce() {}

    StatisticsMapReduce(Set<LabelId> selectedLabelIds, AttributeType.DataType resourceDataType, String degreePropertyKey) {
        super(selectedLabelIds, resourceDataType);
        this.degreePropertyKey = degreePropertyKey;
        this.persistentProperties.put(DegreeVertexProgram.DEGREE, degreePropertyKey);
    }

    @Override
    public void loadState(Graph graph, Configuration configuration) {
        super.loadState(graph, configuration);
        degreePropertyKey = (String) persistentProperties.get(DegreeVertexProgram.DEGREE);
    }

    boolean resourceIsValid(Vertex vertex) {
        return vertexHasSelectedTypeId(vertex, selectedTypes) && vertex.<Long>value(degreePropertyKey) > 0;
    }
}
