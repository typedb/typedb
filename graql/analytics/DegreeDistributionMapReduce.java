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

import grakn.core.core.Schema;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.LabelId;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static grakn.core.graql.analytics.Utility.reduceSet;
import static grakn.core.graql.analytics.Utility.vertexHasSelectedTypeId;

/**
 * The MapReduce program for collecting the result of a degree query.
 * <p>
 * It returns a map, the key being the degree, the value being a vertex id set containing all the vertices
 * with the given degree.
 * <p>
 *
 */

public class DegreeDistributionMapReduce extends GraknMapReduce<Set<ConceptId>> {

    // Needed internally for OLAP tasks
    public DegreeDistributionMapReduce() {
    }

    public DegreeDistributionMapReduce(Set<LabelId> selectedLabelIds, String degreePropertyKey) {
        super(selectedLabelIds);
        this.persistentProperties.put(DegreeVertexProgram.DEGREE, degreePropertyKey);
    }

    @Override
    public void safeMap(final Vertex vertex, final MapEmitter<Serializable, Set<ConceptId>> emitter) {
        if (vertex.property((String) persistentProperties.get(DegreeVertexProgram.DEGREE)).isPresent() &&
                vertexHasSelectedTypeId(vertex, selectedTypes)) {
            String degreePropertyKey = (String) persistentProperties.get(DegreeVertexProgram.DEGREE);
            Long centralityCount = vertex.value(degreePropertyKey);
            ConceptId conceptId = Schema.conceptId(vertex);

            emitter.emit(centralityCount, Collections.singleton(conceptId));
        } else {
            emitter.emit(NullObject.instance(), Collections.emptySet());
        }
    }

    @Override
    Set<ConceptId> reduceValues(Iterator<Set<ConceptId>> values) {
        return reduceSet(values);
    }

    @Override
    public Map<Serializable, Set<ConceptId>> generateFinalResult(Iterator<KeyValue<Serializable, Set<ConceptId>>> keyValues) {
        LOGGER.debug("MapReduce Finished !!!!!!!!");
        final Map<Serializable, Set<ConceptId>> clusterPopulation = Utility.keyValuesToMap(keyValues);
        clusterPopulation.remove(NullObject.instance());
        return clusterPopulation;
    }
}
