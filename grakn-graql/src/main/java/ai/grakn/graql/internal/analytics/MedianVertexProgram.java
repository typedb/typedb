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

/**
 * This class implements quick select algorithm to find the median.
 */

public class MedianVertexProgram extends GraknVertexProgram<Long> {

    public static final int MAX_ITERATION = 40;
    private static final String RESOURCE_DATA_TYPE = "medianVertexProgram.resourceDataType";
    private static final String RESOURCE_TYPE = "medianVertexProgram.statisticsResourceType";

    // element key
    public static final String DEGREE = "medianVertexProgram.degree";
    public static final String LABEL = "medianVertexProgram.label";

    // memory key
    public static final String COUNT = "medianVertexProgram.count";
    public static final String INDEX_START = "medianVertexProgram.indexStart";
    public static final String INDEX_END = "medianVertexProgram.indexEnd";
    public static final String INDEX_MEDIAN = "medianVertexProgram.indexMedian";
    public static final String MEDIAN = "medianVertexProgram.median";
    public static final String PIVOT = "medianVertexProgram.pivot";
    public static final String PIVOT_POSITIVE = "medianVertexProgram.pivotPositive";
    public static final String PIVOT_NEGATIVE = "medianVertexProgram.pivotNegative";
    public static final String POSITIVE_COUNT = "medianVertexProgram.positiveCount";
    public static final String NEGATIVE_COUNT = "medianVertexProgram.negativeCount";
    public static final String FOUND = "medianVertexProgram.found";
    public static final String LABEL_SELECTED = "medianVertexProgram.labelSelected";

    private static final Set<String> ELEMENT_COMPUTE_KEYS = Sets.newHashSet(DEGREE, LABEL);
    private static final Set<String> MEMORY_COMPUTE_KEYS = Sets.newHashSet(COUNT, MEDIAN, FOUND,
            INDEX_START, INDEX_END, INDEX_MEDIAN, PIVOT, PIVOT_POSITIVE, PIVOT_NEGATIVE,
            POSITIVE_COUNT, NEGATIVE_COUNT, LABEL_SELECTED);

    private Set<String> statisticsResourceTypes = new HashSet<>();

    public MedianVertexProgram() {
    }

    public MedianVertexProgram(Set<String> selectedTypes,
                               Set<String> statisticsResourceTypes, String resourceDataType) {
        this.selectedTypes = selectedTypes;
        this.statisticsResourceTypes = statisticsResourceTypes;

        String resourceDataTypeValue = resourceDataType.equals(ResourceType.DataType.LONG.getName()) ?
                Schema.ConceptProperty.VALUE_LONG.name() : Schema.ConceptProperty.VALUE_DOUBLE.name();
        persistentProperties.put(RESOURCE_DATA_TYPE, resourceDataTypeValue);
    }

    @Override
    public Set<String> getElementComputeKeys() {
        return ELEMENT_COMPUTE_KEYS;
    }

    @Override
    public Set<String> getMemoryComputeKeys() {
        return MEMORY_COMPUTE_KEYS;
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        if (memory.getIteration() < 3) return messageScopeSet;
        return Collections.emptySet();
    }

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);
        statisticsResourceTypes.forEach(
                typeId -> configuration.addProperty(RESOURCE_TYPE + "." + typeId, typeId));
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        super.loadState(graph, configuration);
        configuration.subset(RESOURCE_TYPE).getKeys().forEachRemaining(key ->
                statisticsResourceTypes.add((String) configuration.getProperty(RESOURCE_TYPE + "." + key)));
    }

    @Override
    public void setup(final Memory memory) {
        LOGGER.debug("MedianVertexProgram Started !!!!!!!!");
        memory.set(COUNT, 0L);
        memory.set(LABEL_SELECTED, memory.getIteration());
        memory.set(NEGATIVE_COUNT, 0L);
        memory.set(POSITIVE_COUNT, 0L);
        memory.set(FOUND, false);
        if (persistentProperties.get(RESOURCE_DATA_TYPE).equals(Schema.ConceptProperty.VALUE_LONG.name())) {
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
                if (selectedTypes.contains(Utility.getVertexType(vertex))) {
                    degreeStep0(vertex, messenger);
                }
                break;
            case 1:
                if (vertex.label().equals(Schema.BaseType.CASTING.name())) {
                    degreeStep1(messenger);
                }
                break;
            case 2:
                if (statisticsResourceTypes.contains(Utility.getVertexType(vertex))) {
                    // put degree
                    long edgeCount = getEdgeCount(messenger);
                    vertex.property(DEGREE, edgeCount);

                    //TODO: select three values in each iteration, pick the median of the three as pivot
                    // select pivot randomly
                    if (edgeCount > 0) {
                        memory.set(PIVOT,
                                vertex.value((String) persistentProperties.get(RESOURCE_DATA_TYPE)));
                        memory.incr(COUNT, edgeCount);
                    }
                }
                break;
            case 3:
                if (statisticsResourceTypes.contains(Utility.getVertexType(vertex)) &&
                        (long) vertex.value(DEGREE) > 0) {
                    Number value = vertex.value((String) persistentProperties.get(RESOURCE_DATA_TYPE));
                    if (value.doubleValue() < memory.<Number>get(PIVOT).doubleValue()) {
                        vertex.property(LABEL, -memory.getIteration());
                        memory.incr(NEGATIVE_COUNT, vertex.value(DEGREE));
                        memory.set(PIVOT_NEGATIVE, value);
                    } else if (value.doubleValue() > memory.<Number>get(PIVOT).doubleValue()) {
                        vertex.property(LABEL, memory.getIteration());
                        memory.incr(POSITIVE_COUNT, vertex.value(DEGREE));
                        memory.set(PIVOT_POSITIVE, value);
                    } else {
                        // also assign a label to pivot, so all the selected resources have LABEL
                        vertex.property(LABEL, 0);
                    }
                }
                break;

            // default case is almost the same as case 3, except that in case 3 no vertex has LABEL
            default:
                if (statisticsResourceTypes.contains(Utility.getVertexType(vertex)) &&
                        (long) vertex.value(DEGREE) > 0 &&
                        (int) vertex.value(LABEL) == memory.<Integer>get(LABEL_SELECTED)) {
                    Number value = vertex.value((String) persistentProperties.get(RESOURCE_DATA_TYPE));
                    if (value.doubleValue() < memory.<Number>get(PIVOT).doubleValue()) {
                        vertex.property(LABEL, -memory.getIteration());
                        memory.incr(NEGATIVE_COUNT, vertex.value(DEGREE));
                        memory.set(PIVOT_NEGATIVE, value);
                    } else if (value.doubleValue() > memory.<Number>get(PIVOT).doubleValue()) {
                        vertex.property(LABEL, memory.getIteration());
                        memory.incr(POSITIVE_COUNT, vertex.value(DEGREE));
                        memory.set(PIVOT_POSITIVE, value);
                    }
                }
                break;
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        LOGGER.info("Finished Iteration " + memory.getIteration());

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
