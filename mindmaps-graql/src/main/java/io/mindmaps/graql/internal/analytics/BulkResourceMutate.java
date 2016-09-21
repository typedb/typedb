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
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.factory.MindmapsClient;
import io.mindmaps.graql.internal.util.GraqlType;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Methods to deal with persisting values to a Mindmaps graph during OLAP computations. Each spark executor is thread
 * bound and responsible for a subset of the vertices in the graph. Therefore, an instance of the graph can be held in
 * each executor and the mutations from multiple vertices committed as batches. The need to delete relations in a
 * separate iterations from when new relations are added can also be facilitated.
 *
 * Each vertex program should instatiate the <code>BulkResourceMutate</code> class at the start of an iteration and call
 * the <code>close</code> method at the end of each iteration. Additionally <code>cleanup</code> must be called in a
 * separate iteration from <code>putValue</code> to ensure that the graph remains sound.
 */

public class BulkResourceMutate <T>{

    int batchSize = 1;
    boolean havePutValue = false;
    boolean haveCalledCleanup = false;
    private MindmapsGraph graph;
    private int currentNumberOfVertices = 0;
    private String resourceTypeId = Analytics.degree;
    private String keyspace;

    private ResourceType<T> resourceType;
    private RoleType resourceOwner;
    private RoleType resourceValue;
    private RelationType relationType;

    public BulkResourceMutate(String keyspace) {
        this.keyspace = keyspace;
    }

    public BulkResourceMutate(String keyspace, int batchSize) {
        this(keyspace);
        this.batchSize = batchSize;
    }

    void putValue(Vertex vertex,T value, String deleteKey) {
        if (haveCalledCleanup) throw new RuntimeException(ErrorMessage.CANNOT_DELETE_AND_ADD.getMessage());
        havePutValue = true;
        currentNumberOfVertices++;
        initialiseGraph();

        String assertionId = persistResource(vertex, value);
        if (assertionId!=null) vertex.property(deleteKey,assertionId);

        if (currentNumberOfVertices >= batchSize) flush();
    }

    void cleanup(Vertex vertex, String deleteKey) {
        if (havePutValue) throw new RuntimeException(ErrorMessage.CANNOT_DELETE_AND_ADD.getMessage());
        haveCalledCleanup = true;
        currentNumberOfVertices++;
        initialiseGraph();

        if(vertex.property(deleteKey).isPresent()) {
            graph.getRelation(vertex.value(deleteKey)).delete();
            try {
                graph.commit();
            } catch (MindmapsValidationException e) {
                throw new RuntimeException("Failed to delete relation during bulk resource mutation.",e);
            }
        }

        if (currentNumberOfVertices >= batchSize) flush();
    }

    /**
     * Commit all remaining operations and close the connection to the graph.
     */
    void close() {
        flush();
    }

    /**
     * Force all pending operations in the batch to be committed.
     */
    private void flush() {
        initialiseGraph();
        try {
            graph.commit();
        } catch (Exception e) {
            throw new RuntimeException(ErrorMessage.BULK_PERSIST.getMessage(resourceTypeId,e.getMessage()),e);
        }
        currentNumberOfVertices = 0;
        refreshOntologyElements();
    }

    private void refreshOntologyElements() {
        resourceType = graph.getResourceType(resourceTypeId);
        resourceOwner = graph.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceTypeId));
        resourceValue = graph.getRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceTypeId));
        relationType = graph.getRelationType(GraqlType.HAS_RESOURCE.getId(resourceTypeId));
    }

    private void initialiseGraph() {
        if (graph == null) {
            graph = MindmapsClient.getGraphBatchLoading(keyspace);
            refreshOntologyElements();
        }
    }

    /**
     * Adds an instruction to the current batch to relate a resource of <code>value</code> to the concept represented by
     * the <code>vertex</code>. If there is an existing relation to a resource of the same type but with a different
     * value its ID is returned for removal at a later stage.
     *
     * @param vertex    the vertex to which the value should be attached
     * @param value     the value to attach to the vertex
     * @return          the ID of the old relation to be removed
     */
    private String persistResource(Vertex vertex, T value) {
        Instance instance =
                graph.getInstance(vertex.value(Schema.ConceptProperty.ITEM_IDENTIFIER.name()));

        List<Relation> relations = instance.relations(resourceOwner).stream()
                .filter(relation -> relation.rolePlayers().size() == 2)
                .filter(relation -> relation.rolePlayers().containsKey(resourceValue) &&
                        relation.rolePlayers().get(resourceValue).type().getId().equals(resourceTypeId))
                .collect(Collectors.toList());

        if (relations.isEmpty()) {
            Resource<T> resource = graph.putResource(value, resourceType);

            graph.addRelation(relationType)
                    .putRolePlayer(resourceOwner, instance)
                    .putRolePlayer(resourceValue, resource);

            return null;
        }

        relations = relations.stream()
                .filter(relation ->
                        (T) relation.rolePlayers().get(resourceValue).asResource().getValue() != value)
                .collect(Collectors.toList());

        if (!relations.isEmpty()) {
            String oldAssertionId = relations.get(0).getId();

            Resource<T> resource = graph.putResource(value, resourceType);

            graph.addRelation(relationType)
                    .putRolePlayer(resourceOwner, instance)
                    .putRolePlayer(resourceValue, resource);

            return oldAssertionId;
        } else {
            return null;
        }
    }
}
