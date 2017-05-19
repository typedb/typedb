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

import ai.grakn.concept.TypeId;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Set;

/**
 * The abstract MapReduce program for computing statistics.
 * <p>
 *
 * @author Jason Liu
 */

abstract class StatisticsMapReduce<T> extends GraknMapReduce<T> {

    String degreeKey;

    StatisticsMapReduce() {

    }

    StatisticsMapReduce(Set<TypeId> selectedTypeIds, String resourceDataType, String degreeKey) {
        super(selectedTypeIds, resourceDataType);
        this.degreeKey = degreeKey;
        this.persistentProperties.put(DegreeVertexProgram.DEGREE, degreeKey);
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        super.loadState(graph, configuration);
        degreeKey = (String) persistentProperties.get(DegreeVertexProgram.DEGREE);
    }

    boolean resourceIsValid(Vertex vertex) {
        boolean isSelected = selectedTypes.contains(Utility.getVertexTypeId(vertex));
        return isSelected && vertex.<Long>value(degreeKey) > 0;
    }
}
