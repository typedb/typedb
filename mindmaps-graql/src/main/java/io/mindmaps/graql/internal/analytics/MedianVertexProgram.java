/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.analytics;

import com.google.common.collect.Sets;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.util.Schema;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;

public class MedianVertexProgram extends MindmapsVertexProgram<Long> {

    private final MessageScope.Local<Long> countMessageScopeIn = MessageScope.Local.of(__::inE);
    private final MessageScope.Local<Long> countMessageScopeOut = MessageScope.Local.of(__::outE);

    public static final int MAX_ITERATION = 10;
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
//    public static final String PIVOT_POSITIVE_SAVED = "medianVertexProgram.pivotPositiveSaved";
    public static final String PIVOT_NEGATIVE = "medianVertexProgram.pivotNegative";
//    public static final String PIVOT_NEGATIVE_SAVED = "medianVertexProgram.pivotNegativeSaved";
    public static final String POSITIVE_COUNT = "medianVertexProgram.positiveCount";
    public static final String NEGATIVE_COUNT = "medianVertexProgram.negativeCount";
    public static final String FOUND = "medianVertexProgram.found";
    public static final String LABEL_SELECTED = "medianVertexProgram.labelSelected";

    private static final Set<String> ELEMENT_COMPUTE_KEYS = Sets.newHashSet(DEGREE, LABEL);
    private static final Set<String> MEMORY_COMPUTE_KEYS = Sets.newHashSet(COUNT, MEDIAN, FOUND,
            INDEX_START, INDEX_END, INDEX_MEDIAN, PIVOT, PIVOT_POSITIVE, PIVOT_NEGATIVE,
//            PIVOT_POSITIVE_SAVED, PIVOT_NEGATIVE_SAVED,
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
    public GraphComputer.Persist getPreferredPersist() {
        return GraphComputer.Persist.VERTEX_PROPERTIES;
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
        final Set<MessageScope> set = new HashSet<>();
        set.add(this.countMessageScopeOut);
        set.add(this.countMessageScopeIn);
        return set;
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

//    @Override
//    public void workerIterationStart(final Memory memory) {
//        memory.set(PIVOT_POSITIVE_SAVED, 0L);
//        memory.set(PIVOT_NEGATIVE_SAVED, 0L);
//    }

//    @Override
//    public void workerIterationEnd(final Memory memory) {
//        System.out.println("PIVOT_NEGATIVE_SAVED = " + memory.<Long>get(PIVOT_NEGATIVE_SAVED));
//        System.out.println("PIVOT_NEGATIVE = " + memory.<Long>get(PIVOT_NEGATIVE));
//    }

    @Override
    public void setup(final Memory memory) {
        System.out.println();
        System.out.println("Start !!!!!!!!");
        System.out.println();
        memory.set(COUNT, 0L);
        memory.set(MEDIAN, 0L);
//        memory.set(PIVOT_POSITIVE_SAVED, 0L);
//        memory.set(PIVOT_NEGATIVE_SAVED, 0L);

        memory.set(LABEL_SELECTED, memory.getIteration());
        memory.set(PIVOT, 0L);
        memory.set(PIVOT_NEGATIVE, 0L);
        memory.set(PIVOT_POSITIVE, 0L);
        memory.set(NEGATIVE_COUNT, 0L);
        memory.set(POSITIVE_COUNT, 0L);
        memory.set(FOUND, false);
    }

    @Override
    public void safeExecute(final Vertex vertex, Messenger<Long> messenger, final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                if (selectedTypes.contains(getVertexType(vertex))) {
                    String type = vertex.value(Schema.ConceptProperty.BASE_TYPE.name());
                    if (type.equals(Schema.BaseType.ENTITY.name()) || type.equals(Schema.BaseType.RESOURCE.name())) {
                        messenger.sendMessage(this.countMessageScopeIn, 1L);
                    } else if (type.equals(Schema.BaseType.RELATION.name())) {
                        messenger.sendMessage(this.countMessageScopeIn, 1L);
                        messenger.sendMessage(this.countMessageScopeOut, -1L);
                    }
                }
                break;
            case 1:
                String type = vertex.value(Schema.ConceptProperty.BASE_TYPE.name());
                if (type.equals(Schema.BaseType.CASTING.name())) {
                    boolean hasRolePlayer = false;
                    long assertionCount = 0;
                    Iterator<Long> iterator = messenger.receiveMessages();
                    while (iterator.hasNext()) {
                        long message = iterator.next();
                        if (message < 0) assertionCount++;
                        else hasRolePlayer = true;
                    }
                    if (hasRolePlayer) {
                        messenger.sendMessage(this.countMessageScopeIn, 1L);
                        messenger.sendMessage(this.countMessageScopeOut, assertionCount);
                    }
                }
                break;
            case 2:
                if (statisticsResourceTypes.contains(getVertexType(vertex))) {
                    // put degree
                    long edgeCount = IteratorUtils.reduce(messenger.receiveMessages(), 0L, (a, b) -> a + b);
                    vertex.property(DEGREE, edgeCount);
                    // select pivot randomly
                    if (edgeCount > 0) {
                        memory.set(PIVOT,
                                vertex.value((String) persistentProperties.get(RESOURCE_DATA_TYPE)));
                        memory.incr(COUNT, edgeCount);
                    }
                }
                break;
            case 3:
                if (statisticsResourceTypes.contains(getVertexType(vertex)) && (long) vertex.value(DEGREE) > 0) {
                    long value = vertex.value((String) persistentProperties.get(RESOURCE_DATA_TYPE));
                    if (value < memory.<Long>get(PIVOT)) {
                        vertex.property(LABEL, -memory.getIteration());
                        memory.incr(NEGATIVE_COUNT, vertex.value(DEGREE));
                        memory.set(PIVOT_NEGATIVE, value);
                    } else if (value > memory.<Long>get(PIVOT)) {
                        vertex.property(LABEL, memory.getIteration());
                        memory.incr(POSITIVE_COUNT, vertex.value(DEGREE));
                        memory.set(PIVOT_POSITIVE, value);
                    } else {
                        vertex.property(LABEL, 0);
                    }
                }
                break;
            default:
                if (statisticsResourceTypes.contains(getVertexType(vertex)) && (long) vertex.value(DEGREE) > 0 &&
                        (int) vertex.value(LABEL) == memory.<Integer>get(LABEL_SELECTED)) {

//                    System.out.println("OLD PIVOT = " + memory.<Long>get(PIVOT));

                    long value = vertex.value((String) persistentProperties.get(RESOURCE_DATA_TYPE));
//                    System.out.println("value = " + value);

                    if (value < memory.<Long>get(PIVOT)) {
                        vertex.property(LABEL, -memory.getIteration());
                        memory.incr(NEGATIVE_COUNT, vertex.value(DEGREE));
                        memory.set(PIVOT_NEGATIVE, value);
                    } else if (value > memory.<Long>get(PIVOT)) {
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
        System.out.println();
        System.out.println("Iteration: " + memory.getIteration());
        System.out.println();

        if (memory.getIteration() == 2) {
            memory.set(INDEX_START, 0L);
            memory.set(INDEX_END, memory.<Long>get(COUNT) - 1L);
            memory.set(INDEX_MEDIAN, (memory.<Long>get(COUNT) - 1L) / 2L);

            System.out.println("count: " + memory.<Long>get(COUNT));
            System.out.println("first pivot: " + memory.<Long>get(PIVOT));

        } else if (memory.getIteration() > 2) {

            long indexNegativeEnd = memory.<Long>get(INDEX_START) + memory.<Long>get(NEGATIVE_COUNT) - 1;
            long indexPositiveStart = memory.<Long>get(INDEX_END) - memory.<Long>get(POSITIVE_COUNT) + 1;

            System.out.println("pivot: " + memory.<Long>get(PIVOT));

            System.out.println(memory.<Long>get(INDEX_START) + ", " + indexNegativeEnd);
            System.out.println(indexPositiveStart + ", " + memory.<Long>get(INDEX_END));

            System.out.println("negative count: " + memory.<Long>get(NEGATIVE_COUNT));
            System.out.println("positive count: " + memory.<Long>get(POSITIVE_COUNT));

            System.out.println("negative pivot: " + memory.<Long>get(PIVOT_NEGATIVE));
            System.out.println("positive pivot: " + memory.<Long>get(PIVOT_POSITIVE));

            if (indexNegativeEnd < memory.<Long>get(INDEX_MEDIAN)) {
                if (indexPositiveStart > memory.<Long>get(INDEX_MEDIAN)) {
                    memory.set(FOUND, true);
                    System.out.println("FOUND IT!!!");
                } else {
                    memory.set(INDEX_START, indexPositiveStart);
                    memory.set(PIVOT, memory.<Long>get(PIVOT_POSITIVE));
                    memory.set(LABEL_SELECTED, memory.getIteration());
                    System.out.println("new pivot: " + memory.<Long>get(PIVOT));
                }
            } else {
                memory.set(INDEX_END, indexNegativeEnd);
                memory.set(PIVOT, memory.<Long>get(PIVOT_NEGATIVE));
                memory.set(LABEL_SELECTED, -memory.getIteration());
                System.out.println("new pivot: " + memory.<Long>get(PIVOT));
            }
            memory.set(MEDIAN, memory.<Long>get(PIVOT));

            memory.set(POSITIVE_COUNT, 0L);
            memory.set(NEGATIVE_COUNT, 0L);
        }
        return memory.<Boolean>get(FOUND) || memory.getIteration() >= MAX_ITERATION;
    }
}
