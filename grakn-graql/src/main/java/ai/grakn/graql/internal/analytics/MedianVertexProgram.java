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
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static ai.grakn.graql.internal.analytics.DegreeStatisticsVertexProgram.degreeStatisticsStepResourceOwner;
import static ai.grakn.graql.internal.analytics.DegreeStatisticsVertexProgram.degreeStatisticsStepResourceRelation;
import static ai.grakn.graql.internal.analytics.Utility.vertexHasSelectedTypeId;

/**
 * The vertex program for computing the median of given resource using quick select algorithm.
 * <p>
 *
 * @author Jason Liu
 * @author Sheldon Hall
 */

public class MedianVertexProgram extends GraknVertexProgram<Long> {

    private static final int MAX_ITERATION = 40;
    private static final String RESOURCE_DATA_TYPE = "medianVertexProgram.resourceDataType";
    private static final String RESOURCE_TYPE = "medianVertexProgram.statisticsResourceType";

    // element key
    private static final String LABEL = "medianVertexProgram.label";

    // memory key
    public static final String MEDIAN = "medianVertexProgram.median";
    private static final String COUNT = "medianVertexProgram.count";
    private static final String INDEX_START = "medianVertexProgram.indexStart";
    private static final String INDEX_END = "medianVertexProgram.indexEnd";
    private static final String INDEX_MEDIAN = "medianVertexProgram.indexMedian";
    private static final String PIVOT = "medianVertexProgram.pivot";
    private static final String PIVOT_POSITIVE = "medianVertexProgram.pivotPositive";
    private static final String PIVOT_NEGATIVE = "medianVertexProgram.pivotNegative";
    private static final String POSITIVE_COUNT = "medianVertexProgram.positiveCount";
    private static final String NEGATIVE_COUNT = "medianVertexProgram.negativeCount";
    private static final String FOUND = "medianVertexProgram.found";
    private static final String LABEL_SELECTED = "medianVertexProgram.labelSelected";

    private static final Set<String> MEMORY_COMPUTE_KEYS = Sets.newHashSet(COUNT, MEDIAN, FOUND,
            INDEX_START, INDEX_END, INDEX_MEDIAN, PIVOT, PIVOT_POSITIVE, PIVOT_NEGATIVE,
            POSITIVE_COUNT, NEGATIVE_COUNT, LABEL_SELECTED);

    private Set<LabelId> statisticsResourceLabelIds = new HashSet<>();

    private String degreePropertyKey;
    private String labelKey;

    // Needed internally for OLAP tasks
    public MedianVertexProgram() {
    }

    public MedianVertexProgram(Set<LabelId> selectedLabelId, Set<LabelId> statisticsResourceLabelIds,
                               ResourceType.DataType resourceDataType, String randomId) {
        this.selectedTypes = selectedLabelId;
        this.statisticsResourceLabelIds = statisticsResourceLabelIds;

        String resourceDataTypeValue = resourceDataType.equals(ResourceType.DataType.LONG) ?
                Schema.VertexProperty.VALUE_LONG.name() : Schema.VertexProperty.VALUE_DOUBLE.name();
        persistentProperties.put(RESOURCE_DATA_TYPE, resourceDataTypeValue);

        degreePropertyKey = DegreeVertexProgram.DEGREE + randomId;
        labelKey = LABEL + randomId;
        persistentProperties.put(DegreeVertexProgram.DEGREE, degreePropertyKey);
        persistentProperties.put(LABEL, labelKey);
    }

    @Override
    public Set<String> getElementComputeKeys() {
        return Sets.newHashSet(degreePropertyKey, labelKey);
    }

    @Override
    public Set<String> getMemoryComputeKeys() {
        return MEMORY_COMPUTE_KEYS;
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                return Collections.singleton(messageScopeShortcutIn);
            case 1:
                return Collections.singleton(messageScopeShortcutOut);
            default:
                return Collections.emptySet();
        }
    }

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);
        statisticsResourceLabelIds.forEach(
                typeId -> configuration.addProperty(RESOURCE_TYPE + "." + typeId, typeId));
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        super.loadState(graph, configuration);
        configuration.subset(RESOURCE_TYPE).getKeys().forEachRemaining(key ->
                statisticsResourceLabelIds.add(LabelId.of(configuration.getInt(RESOURCE_TYPE + "." + key))));

        degreePropertyKey = (String) this.persistentProperties.get(DegreeVertexProgram.DEGREE);
        labelKey = (String) this.persistentProperties.get(LABEL);
    }

    @Override
    public void setup(final Memory memory) {
        LOGGER.debug("MedianVertexProgram Started !!!!!!!!");
        memory.set(COUNT, 0L);
        memory.set(LABEL_SELECTED, memory.getIteration());
        memory.set(NEGATIVE_COUNT, 0L);
        memory.set(POSITIVE_COUNT, 0L);
        memory.set(FOUND, false);
        if (persistentProperties.get(RESOURCE_DATA_TYPE).equals(Schema.VertexProperty.VALUE_LONG.name())) {
            memory.set(MEDIAN, 0L);
            memory.set(PIVOT, 0L);
            memory.set(PIVOT_NEGATIVE, 0L);
            memory.set(PIVOT_POSITIVE, 0L);
        } else {
            memory.set(MEDIAN, 0D);
            memory.set(PIVOT, 0D);
            memory.set(PIVOT_NEGATIVE, 0D);
            memory.set(PIVOT_POSITIVE, 0D);
        }
    }

    @Override
    public void safeExecute(final Vertex vertex, Messenger<Long> messenger, final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                degreeStatisticsStepResourceOwner(vertex, messenger, selectedTypes, statisticsResourceLabelIds);
                break;
            case 1:
                degreeStatisticsStepResourceRelation(vertex, messenger);
                break;
            case 2:
                if (vertexHasSelectedTypeId(vertex, statisticsResourceLabelIds)) {
                    // put degree
                    long degree = getMessageCount(messenger);
                    vertex.property(degreePropertyKey, degree);
                    // select pivot randomly
                    if (degree > 0) {
                        memory.set(PIVOT,
                                vertex.value((String) persistentProperties.get(RESOURCE_DATA_TYPE)));
                        memory.incr(COUNT, degree);
                    }
                }
                break;
            case 3:
                if (vertexHasSelectedTypeId(vertex, statisticsResourceLabelIds) &&
                        (long) vertex.value(degreePropertyKey) > 0) {
                    Number value = vertex.value((String) persistentProperties.get(RESOURCE_DATA_TYPE));
                    if (value.doubleValue() < memory.<Number>get(PIVOT).doubleValue()) {
                        updateMemoryNegative(vertex, memory, value);
                    } else if (value.doubleValue() > memory.<Number>get(PIVOT).doubleValue()) {
                        updateMemoryPositive(vertex, memory, value);
                    } else {
                        // also assign a label to pivot, so all the selected resources have label
                        vertex.property(labelKey, 0);
                    }
                }
                break;

            // default case is almost the same as case 5, except that in case 5 no vertex has label
            default:
                if (vertexHasSelectedTypeId(vertex, statisticsResourceLabelIds) &&
                        (long) vertex.value(degreePropertyKey) > 0 &&
                        (int) vertex.value(labelKey) == memory.<Integer>get(LABEL_SELECTED)) {
                    Number value = vertex.value((String) persistentProperties.get(RESOURCE_DATA_TYPE));
                    if (value.doubleValue() < memory.<Number>get(PIVOT).doubleValue()) {
                        updateMemoryNegative(vertex, memory, value);
                    } else if (value.doubleValue() > memory.<Number>get(PIVOT).doubleValue()) {
                        updateMemoryPositive(vertex, memory, value);
                    }
                }
                break;
        }
    }

    private void updateMemoryPositive(Vertex vertex, Memory memory, Number value) {
        vertex.property(labelKey, memory.getIteration());
        memory.incr(POSITIVE_COUNT, vertex.value(degreePropertyKey));
        memory.set(PIVOT_POSITIVE, value);
    }

    private void updateMemoryNegative(Vertex vertex, Memory memory, Number value) {
        vertex.property(labelKey, -memory.getIteration());
        memory.incr(NEGATIVE_COUNT, vertex.value(degreePropertyKey));
        memory.set(PIVOT_NEGATIVE, value);
    }

    @Override
    public boolean terminate(final Memory memory) {
        LOGGER.debug("Finished Iteration " + memory.getIteration());

        if (memory.getIteration() == 2) {
            memory.set(INDEX_START, 0L);
            memory.set(INDEX_END, memory.<Long>get(COUNT) - 1L);
            memory.set(INDEX_MEDIAN, (memory.<Long>get(COUNT) - 1L) / 2L);

            LOGGER.debug("count: " + memory.<Long>get(COUNT));
            LOGGER.debug("first pivot: " + memory.<Long>get(PIVOT));

        } else if (memory.getIteration() > 2) {

            long indexNegativeEnd = memory.<Long>get(INDEX_START) + memory.<Long>get(NEGATIVE_COUNT) - 1;
            long indexPositiveStart = memory.<Long>get(INDEX_END) - memory.<Long>get(POSITIVE_COUNT) + 1;

            LOGGER.debug("pivot: " + memory.get(PIVOT));

            LOGGER.debug(memory.<Long>get(INDEX_START) + ", " + indexNegativeEnd);
            LOGGER.debug(indexPositiveStart + ", " + memory.<Long>get(INDEX_END));

            LOGGER.debug("negative count: " + memory.<Long>get(NEGATIVE_COUNT));
            LOGGER.debug("positive count: " + memory.<Long>get(POSITIVE_COUNT));

            LOGGER.debug("negative pivot: " + memory.get(PIVOT_NEGATIVE));
            LOGGER.debug("positive pivot: " + memory.get(PIVOT_POSITIVE));

            if (indexNegativeEnd < memory.<Long>get(INDEX_MEDIAN)) {
                if (indexPositiveStart > memory.<Long>get(INDEX_MEDIAN)) {
                    memory.set(FOUND, true);
                    LOGGER.debug("FOUND IT!!!");
                } else {
                    memory.set(INDEX_START, indexPositiveStart);
                    memory.set(PIVOT, memory.get(PIVOT_POSITIVE));
                    memory.set(LABEL_SELECTED, memory.getIteration());
                    LOGGER.debug("new pivot: " + memory.get(PIVOT));
                }
            } else {
                memory.set(INDEX_END, indexNegativeEnd);
                memory.set(PIVOT, memory.get(PIVOT_NEGATIVE));
                memory.set(LABEL_SELECTED, -memory.getIteration());
                LOGGER.debug("new pivot: " + memory.get(PIVOT));
            }
            memory.set(MEDIAN, memory.get(PIVOT));

            memory.set(POSITIVE_COUNT, 0L);
            memory.set(NEGATIVE_COUNT, 0L);
        }

        return memory.<Boolean>get(FOUND) || memory.getIteration() >= MAX_ITERATION;
    }
}
