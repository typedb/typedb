/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Ltd
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

package ai.grakn.test.engine.postprocessing;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.engine.lock.ProcessWideLockProvider;
import ai.grakn.engine.postprocessing.PostProcessingTask;
import ai.grakn.engine.tasks.manager.TaskConfiguration;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.engine.tasks.manager.TaskSubmitter;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.test.EngineContext;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Sets;
import mjson.Json;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static ai.grakn.test.engine.postprocessing.PostProcessingTestUtils.createDuplicateResource;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.Schema.VertexProperty.INDEX;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class PostProcessingTest {

    private GraknSession session;

    private TaskSubmitter mockTaskSubmitter = new TaskSubmitter() {
        @Override
        public void addTask(TaskState taskState, TaskConfiguration configuration) {
        }

        @Override
        public void runTask(TaskState taskState, TaskConfiguration configuration) {
        }
    };

    @ClassRule
    public static final EngineContext engine = EngineContext.createWithInMemoryRedis();

    @BeforeClass
    public static void onlyRunOnTinker(){
        assumeTrue(GraknTestSetup.usingTinker());
    }

    @Before
    public void setUp() throws Exception {
        session = engine.sessionWithNewKeyspace();
    }

    @After
    public void takeDown() throws InterruptedException {
        session.close();
    }

    @Ignore("Get this test working")
    @Test
    public void whenCreatingRelationshipsWithDuplicateRolePlayers_EnsureTheyAreMergedInPost(){
        final int NUM_RELATIONSHIPS = 10;
        String entityTypeLabel = "Some Type";
        String relationshipTypeLabel = "Some Relationship Type";
        String role1Label = "role 1";
        String role2Label = "role 2";
        Keyspace keyspace;

        //Create a simple ontology
        try(GraknTx tx = session.open(GraknTxType.WRITE)){
            keyspace = tx.getKeyspace();
            Role role1 = tx.putRole(role1Label);
            Role role2 = tx.putRole(role2Label);
            tx.putEntityType(entityTypeLabel).plays(role1).plays(role2);
            tx.putRelationshipType(relationshipTypeLabel).relates(role1).relates(role2);
            tx.admin().commitNoLogs();
        }

        //Create Relationships With Duplicate Role Players
        //NOTE: The transaction must be batch. Otherwise duplicate role players cannot be created
        Set<String> relationshipIds = new HashSet<>();
        try(GraknTx tx = session.open(GraknTxType.BATCH)){
            EntityType entityType = tx.getEntityType(entityTypeLabel);
            Entity e1 = entityType.addEntity();
            Entity e2 = entityType.addEntity();

            Role role1 = tx.getRole(role1Label);
            Role role2 = tx.getRole(role2Label);
            RelationshipType relationshipType = tx.getRelationshipType(relationshipTypeLabel);

            //Create relationships with random number of role players
            //In the end though after PP there must only be 4 role players to each relation
            for(int i = 0; i < NUM_RELATIONSHIPS; i ++){
                relationshipIds.add(createRelationshipWithRandomlyDuplicateRolePlayers(relationshipType, role1, role2, e1, e2).getId().getValue());
            }

            tx.admin().commitNoLogs();
        }

        //Create the Task To Fix Everything
        PostProcessingTask task = new PostProcessingTask();
        TaskConfiguration configuration = TaskConfiguration.of(
                Json.object(
                        KEYSPACE, keyspace.getValue(),
                        REST.Request.COMMIT_LOG_FIXING, Json.object(
                                Schema.BaseType.RELATIONSHIP.name(), Json.array(relationshipIds.toArray())
                        ))
        );
        task.initialize(null, configuration, mockTaskSubmitter, engine.config(), null, engine.server().factory(),
                new ProcessWideLockProvider(), new MetricRegistry());

        task.start();

        //Check that everything is fixed
        try(GraknTx tx = session.open(GraknTxType.BATCH)){
            tx.admin().getMetaRelationType().instances().forEach(relationship -> {
                assertEquals(4, relationship.rolePlayers().collect(Collectors.toList()).size());
            });
        }
    }

    private Relationship createRelationshipWithRandomlyDuplicateRolePlayers(RelationshipType relationshipType, Role role1, Role role2, Thing thing1, Thing thing2){
        Relationship relationship = relationshipType.addRelationship();
        randomRolePlayers(relationship, role1, thing1);
        randomRolePlayers(relationship, role1, thing2);
        randomRolePlayers(relationship, role2, thing1);
        randomRolePlayers(relationship, role2, thing2);
        return relationship;
    }

    private void randomRolePlayers(Relationship relationship, Role role, Thing thing){
        for(int i = 0; i < ThreadLocalRandom.current().nextInt(1, 5); i++){
            relationship.addRolePlayer(role, thing);
        }
    }

    @Test
    public void whenCreatingDuplicateResources_EnsureTheyAreMergedInPost() throws InvalidKBException, InterruptedException {
        String value = "1";
        String sample = "Sample";

        //Create GraknTx With Duplicate Resources
        GraknTx tx = session.open(GraknTxType.WRITE);
        AttributeType<String> attributeType = tx.putAttributeType(sample, AttributeType.DataType.STRING);

        Attribute<String> attribute = attributeType.putAttribute(value);
        tx.admin().commitNoLogs();
        tx = session.open(GraknTxType.WRITE);

        assertEquals(1, attributeType.instances().count());
        //Check duplicates have been created
        Set<Vertex> resource1 = createDuplicateResource(tx, attributeType, attribute);
        Set<Vertex> resource2 = createDuplicateResource(tx, attributeType, attribute);
        Set<Vertex> resource3 = createDuplicateResource(tx, attributeType, attribute);
        Set<Vertex> resource4 = createDuplicateResource(tx, attributeType, attribute);
        assertEquals(5, attributeType.instances().count());

        // Attribute vertex index
        String resourceIndex = resource1.iterator().next().value(INDEX.name()).toString();

        // Merge the attribute sets
        Set<Vertex> merged = Sets.newHashSet();
        merged.addAll(resource1);
        merged.addAll(resource2);
        merged.addAll(resource3);
        merged.addAll(resource4);

        tx.close();

        //Now fix everything

        // Casting sets as ConceptIds
        Set<String> resourceConcepts = merged.stream().map(c -> Schema.PREFIX_VERTEX + c.id().toString()).collect(toSet());

        //Now fix everything
        PostProcessingTask task = new PostProcessingTask();
        TaskConfiguration configuration = TaskConfiguration.of(
                Json.object(
                        KEYSPACE, tx.getKeyspace().getValue(),
                        REST.Request.COMMIT_LOG_FIXING, Json.object(
                                Schema.BaseType.ATTRIBUTE.name(), Json.object(resourceIndex, resourceConcepts)
                        ))
        );
        task.initialize(null, configuration, mockTaskSubmitter, engine.config(), null, engine.server().factory(),
                new ProcessWideLockProvider(), new MetricRegistry());

        task.start();

        tx = session.open(GraknTxType.READ);

        //Check it's fixed
        assertEquals(1, tx.getAttributeType(sample).instances().count());

        tx.close();
    }
}