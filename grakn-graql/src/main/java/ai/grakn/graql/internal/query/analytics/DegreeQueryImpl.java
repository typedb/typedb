/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

import ai.grakn.GraknTx;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.analytics.DegreeQuery;
import ai.grakn.graql.internal.analytics.DegreeDistributionMapReduce;
import ai.grakn.graql.internal.analytics.DegreeVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

class DegreeQueryImpl extends AbstractCentralityQuery<DegreeQuery> implements DegreeQuery {

    DegreeQueryImpl(Optional<GraknTx> graph) {
        this.tx = graph;
    }

    @Override
    protected final Map<Long, Set<String>> innerExecute() {
        initSubGraph();
        getAllSubTypes();

        // Check if ofType is valid before returning emptyMap
        if (ofLabels.isEmpty()) {
            ofLabels.addAll(subLabels);
        } else {
            ofLabels = ofLabels.stream()
                    .flatMap(typeLabel -> {
                        Type type = tx.get().getSchemaConcept(typeLabel);
                        if (type == null) throw GraqlQueryException.labelNotFound(typeLabel);
                        return type.subs();
                    })
                    .map(SchemaConcept::getLabel)
                    .collect(Collectors.toSet());
            subLabels.addAll(ofLabels);
        }

        if (!selectedTypesHaveInstance()) {
            return Collections.emptyMap();
        }

        Set<LabelId> subLabelIds = convertLabelsToIds(subLabels);
        Set<LabelId> ofLabelIds = convertLabelsToIds(ofLabels);

        ComputerResult result = getGraphComputer().compute(
                new DegreeVertexProgram(ofLabelIds),
                new DegreeDistributionMapReduce(ofLabelIds, DegreeVertexProgram.DEGREE),
                subLabelIds);

        return result.memory().get(DegreeDistributionMapReduce.class.getName());
    }

    @Override
    CentralityMeasure getMethod() {
        return CentralityMeasure.DEGREE;
    }

    @Override
    String graqlString() {
        return super.graqlString() + ";";
    }
}
