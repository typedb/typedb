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

import ai.grakn.concept.Concept;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.GraknGraph;
import ai.grakn.concept.RoleType;
import ai.grakn.Grakn;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Methods to deal with persisting values to a Grakn graph during OLAP computations. Each spark executor is thread
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
    private GraknGraph graph;
    private int currentNumberOfVertices = 0;
    private String resourceTypeName;
    private final String keyspace;
    private Map<String, T> resourcesToPersist = new HashMap<>();

    private ResourceType<T> resourceType;
    private RoleType resourceOwner;
    private RoleType resourceValue;
    private RelationType relationType;

    public BulkResourceMutate(String keyspace, String resourceTypeName) {
        LOGGER.debug("Starting BulkResourceMutate");
        this.keyspace = keyspace;
        this.resourceTypeName = resourceTypeName;
    }

    public BulkResourceMutate(String keyspace, String resourceTypeName, int batchSize) {
        this(keyspace, resourceTypeName);
        this.batchSize = batchSize;
    }

    void putValue(Vertex vertex, T value) {
        currentNumberOfVertices++;

        LOGGER.debug("Considering vertex: " + vertex);
        vertex.properties().forEachRemaining(p -> LOGGER.debug("Vertex property: " + p.toString()));

        String id = vertex.id().toString();
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
            LOGGER.debug("Flush called, about to persist");
            try {
                persistResources();
            } catch (Exception e) {
                LOGGER.info("Exception: " + e.getMessage());
                hasFailed = true;
                numberOfFailures++;
                LOGGER.info("Number of failures: " + numberOfFailures);
                if (!(numberOfFailures < numberOfRetries)) {
                    LOGGER.debug("REACHED MAX NUMBER OF RETRIES !!!!!!!!");
                    throw new RuntimeException(
                            ErrorMessage.BULK_PERSIST.getMessage(resourceTypeName, e.getMessage()), e);
                }
            }
        } while (hasFailed);

        resourcesToPersist.clear();
        currentNumberOfVertices = 0;
    }

    private void persistResources() throws GraknValidationException {
        if (resourcesToPersist.isEmpty()) {
            LOGGER.debug("Nothing to persist");
            return;
        }

        initialiseGraph();
        resourcesToPersist.forEach((id, value) -> {
            Instance instance = graph.getConcept(id);

            // fetch all current resource assertions on the instance
            List<Relation> relations = instance.relations(resourceOwner).stream()
                    .filter(relation -> relation.rolePlayers().size() == 2 &&
                            relation.rolePlayers().containsKey(resourceValue))
                    .filter(relation -> {
                        Instance rolePlayer = relation.rolePlayers().get(resourceValue);
                        return rolePlayer == null || rolePlayer.type().getName().equals(resourceTypeName);
                    }).collect(Collectors.toList());

            relations.forEach(relation -> LOGGER.debug("Assertions currently attached: " + relation.toString()));

            // if there are no resources at all make a new one
            if (relations.isEmpty()) {
                LOGGER.debug("Persisting a new assertion");
                Resource<T> resource = resourceType.putResource(value);

                relationType.addRelation()
                        .putRolePlayer(resourceOwner, instance)
                        .putRolePlayer(resourceValue, resource);

                return;
            }

            // check the exact resource type and value doesn't exist already
            relations = relations.stream().filter(relation -> {
                Instance roleplayer = relation.rolePlayers().get(resourceValue);
                return roleplayer == null || roleplayer.asResource().getValue() != value;
            }).collect(Collectors.toList());

            // if it doesn't exist already delete the old one(s) and add the new one
            if (!relations.isEmpty()) {
                LOGGER.debug("Deleting " + relations.size() + " existing assertion(s), adding a new one");
                relations.forEach(Concept::delete);

                Resource<T> resource = resourceType.putResource(value);

                relationType.addRelation()
                        .putRolePlayer(resourceOwner, instance)
                        .putRolePlayer(resourceValue, resource);
            } else LOGGER.debug("Correct assertion already exists");
        });

        graph.commit();
    }

    private void refreshOntologyElements() {
        resourceType = graph.getResourceType(resourceTypeName);
        resourceOwner = graph.getRoleType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceTypeName));
        resourceValue = graph.getRoleType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceTypeName));
        relationType = graph.getRelationType(Schema.Resource.HAS_RESOURCE.getName(resourceTypeName));
    }

    private void initialiseGraph() {
        if (graph == null) {
            graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraphBatchLoading();
            graph.rollback();
            refreshOntologyElements();
        }
    }
}
