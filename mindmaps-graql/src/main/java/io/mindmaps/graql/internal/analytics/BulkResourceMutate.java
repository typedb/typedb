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

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.*;
import io.mindmaps.Mindmaps;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Methods to deal with persisting values to a Mindmaps graph during OLAP computations. Each spark executor is thread
 * bound and responsible for a subset of the vertices in the graph. Therefore, an instance of the graph can be held in
 * each executor and the mutations from multiple vertices committed as batches. The need to delete relations in a
 * separate iterations from when new relations are added can also be facilitated.
 * <p/>
 * Each vertex program should instatiate the <code>BulkResourceMutate</code> class at the start of an iteration and call
 * the <code>close</code> method at the end of each iteration. Additionally <code>cleanup</code> must be called in a
 * separate iteration from <code>putValue</code> to ensure that the graph remains sound.
 */

public class BulkResourceMutate<T> {
    static final Logger LOGGER = LoggerFactory.getLogger(BulkResourceMutate.class);

    private static final int numberOfRetries = 10;

    private int batchSize = 100;
    private MindmapsGraph graph;
    private int currentNumberOfVertices = 0;
    private String resourceTypeId;
    private final String keyspace;
    private Map<String, T> resourcesToPersist = new HashMap<>();

    private ResourceType<T> resourceType;
    private RoleType resourceOwner;
    private RoleType resourceValue;
    private RelationType relationType;

    // This has been added for debugging purposes - set to true for debugging
    private boolean verboseOutput = false;

    public BulkResourceMutate(String keyspace, String resourceTypeId) {
        LOGGER.debug("Starting BulkResourceMutate");
        this.keyspace = keyspace;
        this.resourceTypeId = resourceTypeId;
    }

    public BulkResourceMutate(String keyspace, String resourceTypeId, int batchSize) {
        this(keyspace, resourceTypeId);
        this.batchSize = batchSize;
    }

    void putValue(Vertex vertex, T value) {
        currentNumberOfVertices++;
        initialiseGraph();

        LOGGER.debug("Considering vertex: " + vertex);
        vertex.properties().forEachRemaining(p -> LOGGER.debug("Vertex property: " + p.toString()));

        if (verboseOutput) {
            System.out.println("considering vertex: " + vertex);
            vertex.properties().forEachRemaining(System.out::println);
        }

        String id = vertex.value(Schema.ConceptProperty.ITEM_IDENTIFIER.name());
        resourcesToPersist.put(id, value);

        if (currentNumberOfVertices >= batchSize) flush();
    }

    /**
     * Force all pending operations in the batch to be committed.
     */
    void flush() {
        boolean hasFailed;
        int numberOfFailures = 0;

        do {
            hasFailed = false;
            LOGGER.debug("About to persist");
            try {
                persistResources();
            } catch (Exception e) {
                LOGGER.debug(e.getMessage());
                hasFailed = true;
                numberOfFailures++;
                LOGGER.debug("Number of failures: " + numberOfFailures);
                if (!(numberOfFailures < numberOfRetries)) {
                    LOGGER.debug("REACHED MAX NUMBER OF RETRIES !!!!!!!!");
                    throw new RuntimeException(ErrorMessage.BULK_PERSIST.getMessage(resourceTypeId, e.getMessage()), e);
                }
            }
        } while (hasFailed);

        resourcesToPersist.clear();
        currentNumberOfVertices = 0;
    }

    private void persistResources() throws MindmapsValidationException {
        initialiseGraph();

        resourcesToPersist.forEach((id, value) -> {
            Instance instance =
                    graph.getInstance(id);

            // fetch all current resource assertions on the instance
            List<Relation> relations = instance.relations(resourceOwner).stream()
                    .filter(relation -> relation.rolePlayers().size() == 2)
                    .filter(relation -> {
                        boolean currentLogicalState = relation.rolePlayers().containsKey(resourceValue);
                        Instance roleplayer = relation.rolePlayers().get(resourceValue);
                        if (roleplayer != null) {
                            return roleplayer.type().getId().equals(resourceTypeId) && currentLogicalState;
                        } else {
                            return currentLogicalState;
                        }
                    }).collect(Collectors.toList());

            relations.forEach(relation -> LOGGER.debug("Assertions currently attached: " + relation.toString()));

            if (verboseOutput) {
                System.out.println("assertions currently attached");
                relations.forEach(System.out::println);
            }

            // if there are no resources at all make a new one
            if (relations.isEmpty()) {
                LOGGER.debug("Persisting a new assertion");
                Resource<T> resource = graph.putResource(value, resourceType);

                graph.addRelation(relationType)
                        .putRolePlayer(resourceOwner, instance)
                        .putRolePlayer(resourceValue, resource);

                return;
            }

            // check the exact resource type and value doesn't exist already
            relations = relations.stream().filter(relation -> {
                Instance roleplayer = relation.rolePlayers().get(resourceValue);
                return roleplayer == null || roleplayer.asResource().getValue() != value;
            }).collect(Collectors.toList());

            // if it doesn't exist already delete the old one and add the new one
            // TODO: we need to figure out what to do when we have multiple resources of the same type already in graph
            if (!relations.isEmpty()) {
                LOGGER.debug("Deleting " + relations.size() + " existing assertion(s), adding a new one");
//                graph.getRelation(relations.get(0).getId()).delete();
                relations.forEach(Concept::delete);

                Resource<T> resource = graph.putResource(value, resourceType);

                graph.addRelation(relationType)
                        .putRolePlayer(resourceOwner, instance)
                        .putRolePlayer(resourceValue, resource);
            } else LOGGER.debug("Correct assertion already exists");
        });

        graph.commit();
    }

    private void refreshOntologyElements() {
        resourceType = graph.getResourceType(resourceTypeId);
        resourceOwner = graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getId(resourceTypeId));
        resourceValue = graph.getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getId(resourceTypeId));
        relationType = graph.getRelationType(Schema.Resource.HAS_RESOURCE.getId(resourceTypeId));
    }

    private void initialiseGraph() {
        if (graph == null) {
            graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, keyspace).getGraphBatchLoading();
            graph.rollback();
            refreshOntologyElements();
        }
    }
}
