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

import ai.grakn.Grakn;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.postprocessing.PostProcessingTask;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.exception.PropertyNotUniqueException;
import ai.grakn.kb.internal.GraknTxAbstract;
import ai.grakn.kb.internal.GraknTxJanus;
import ai.grakn.test.EngineContext;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.util.Schema;
import org.janusgraph.core.SchemaViolationException;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assume.assumeFalse;

public class PostProcessingIT {
    private static final Logger LOG = LoggerFactory.getLogger(PostProcessingIT.class);

    @ClassRule
    public static final EngineContext engine = EngineContext.createWithInMemoryRedis();

    //TODO: Fix this test
    @Ignore("Submitted tasks appear to be lost when using redis task manager.")
    @Test
    public void checkThatDuplicateResourcesAtLargerScaleAreMerged() throws InvalidKBException, ExecutionException, InterruptedException {
        assumeFalse(GraknTestSetup.usingTinker());

        GraknSession session = engine.sessionWithNewKeyspace();
        Keyspace keyspace;

        int transactionSize = 50;
        int numAttempts = 100;

        //Attribute Variables
        int numResTypes = 100;
        int numResVar = 100;

        //Entity Variables
        int numEntTypes = 50;
        int numEntVar = 50;

        ExecutorService pool = Executors.newFixedThreadPool(40);
        Set<Future> futures = new HashSet<>();

        try (GraknTx graph = session.open(GraknTxType.WRITE)) {
            keyspace = graph.getKeyspace();

            //Create Simple Schema
            for (int i = 0; i < numEntTypes; i++) {
                EntityType entityType = graph.putEntityType("ent" + i);
                for (int j = 0; j < numEntVar; j++) {
                    entityType.addEntity();
                }
            }

            for (int i = 0; i < numResTypes; i++) {
                AttributeType<Integer> rt = graph.putAttributeType("res" + i, AttributeType.DataType.INTEGER);
                for (int j = 0; j < numEntTypes; j++) {
                    graph.getEntityType("ent" + j).attribute(rt);
                }
            }

            graph.commit();
        }

        for(int i = 0; i < numAttempts; i++){
            GraknSession finalSession = session;
            futures.add(pool.submit(() -> {
                try(GraknTx graph = finalSession.open(GraknTxType.WRITE)){
                    Random r = new Random();

                    for(int j = 0; j < transactionSize; j ++) {
                        int resType = r.nextInt(numResTypes);
                        int resValue = r.nextInt(numResVar);
                        int entType = r.nextInt(numEntTypes);
                        int entNum = r.nextInt(numEntVar);
                        forceDuplicateResources(graph, resType, resValue, entType, entNum);
                    }

                    Thread.sleep((long) Math.floor(Math.random() * 1000));

                    graph.commit();
                } catch (InterruptedException | SchemaViolationException | PropertyNotUniqueException | InvalidKBException e ) {
                    //IGNORED
                }
            }));
        }

        for (Future future : futures) {
            future.get();
        }

        //Waiting for Engine transactions to complete
        GraknTx fakeTx = session.open(GraknTxType.WRITE);
        fakeTx.close();

        StandardJanusGraph internalConnection = (StandardJanusGraph) ((GraknTxJanus) fakeTx).getTinkerPopGraph();
        while(internalConnection.getOpenTransactions().size() > 0){
            LOG.debug("Number of open transaction: " + internalConnection.getOpenTransactions().size());
            Thread.sleep(1000);
        }

        session.close();
        session = Grakn.session(engine.uri(), keyspace);

        //Check current broken state of graph
        assertTrue("Failed at breaking graph", graphIsBroken(session));

        // Check graph fixed
        long tasksRunning = 0;
        do{
            Thread.sleep(1000);

            Set<TaskState> runningPPTasks = engine.getTaskManager().storage().getTasks(null, PostProcessingTask.class.getName(), null, null, 0, 0);
            LOG.debug("Tasks found {}", runningPPTasks.size());
            tasksRunning = runningPPTasks.stream()
                    .filter(t -> !t.status().equals(TaskStatus.COMPLETED)).count();
            LOG.debug("Still running {}", tasksRunning);
        } while(tasksRunning > 0);

        assertFalse("Failed at fixing graph", graphIsBroken(session));

        try(GraknTx graph = session.open(GraknTxType.WRITE)) {
            //Check the resource indices are working
            graph.admin().getMetaResourceType().instances().forEach(object -> {
                Attribute attribute = (Attribute) object;
                String index = Schema.generateAttributeIndex(attribute.type().getLabel(), attribute.getValue().toString());
                assertEquals(attribute, ((GraknTxAbstract<?>) graph).getConcept(Schema.VertexProperty.INDEX, index).get());
            });
        }
    }

    @SuppressWarnings({"unchecked", "SuspiciousMethodCalls"})
    private boolean graphIsBroken(GraknSession session){
        try(GraknTx graph = session.open(GraknTxType.WRITE)) {
            Stream<AttributeType<?>> resourceTypes = graph.admin().getMetaResourceType().subs();
            return resourceTypes.anyMatch(resourceType -> {
                if (!Schema.MetaSchema.ATTRIBUTE.getLabel().equals(resourceType.getLabel())) {
                    Set<Integer> foundValues = new HashSet<>();
                    for (Attribute<?> attribute : resourceType.instances().collect(Collectors.toSet())) {
                        if (foundValues.contains(attribute.getValue())) {
                            LOG.error("Attribute {} still has duplicates", attribute);
                            return true;
                        } else {
                            foundValues.add((Integer) attribute.getValue());
                        }
                    }
                }
                return false;
            });
        }
    }

    private void forceDuplicateResources(GraknTx graph, int resourceTypeNum, int resourceValueNum, int entityTypeNum, int entityNum){
        Attribute attribute = graph.getAttributeType("res" + resourceTypeNum).putAttribute(resourceValueNum);
        Entity entity = (Entity) graph.getEntityType("ent" + entityTypeNum).instances().toArray()[entityNum]; //Randomly pick an entity
        entity.attribute(attribute);
    }
}