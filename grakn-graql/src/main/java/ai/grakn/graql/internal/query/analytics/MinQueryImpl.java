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
import ai.grakn.graql.analytics.MinQuery;
import ai.grakn.graql.internal.analytics.DegreeVertexProgram;
import ai.grakn.graql.internal.analytics.GraknMapReduce;
import ai.grakn.graql.internal.analytics.MinMapReduce;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class MinQueryImpl extends AbstractStatisticsQuery<Optional<Number>> implements MinQuery {

    @Override
    public Optional<Number> execute() {
        LOGGER.info("MinMapReduce is called");
        initSubGraph();

        String dataType = checkSelectedResourceTypesHaveCorrectDataType(statisticsResourceTypeNames);
        if (!selectedResourceTypesHaveInstance(statisticsResourceTypeNames)) return Optional.empty();

        Set<String> allSubtypes = statisticsResourceTypeNames.stream()
                .map(Schema.Resource.HAS_RESOURCE::getId).collect(Collectors.toSet());
        allSubtypes.addAll(subTypeNames);
        allSubtypes.addAll(statisticsResourceTypeNames);

        GraknComputer computer = getGraphComputer();
        ComputerResult result = computer.compute(new DegreeVertexProgram(allSubtypes),
                new MinMapReduce(statisticsResourceTypeNames, dataType));
        Map<String, Number> min = result.memory().get(GraknMapReduce.MAP_REDUCE_MEMORY_KEY);
        LOGGER.info("MinMapReduce is done");
        return Optional.of(min.get(MinMapReduce.MEMORY_KEY));
    }

    @Override
    public MinQuery of(String... resourceTypeNames) {
        return (MinQuery) setStatisticsResourceType(resourceTypeNames);
    }

}
