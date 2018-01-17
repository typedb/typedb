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

package ai.grakn.test.engine.postprocessing;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.engine.postprocessing.PostProcessingTask;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.kb.log.CommitLog;
import ai.grakn.test.rule.EngineContext;
import ai.grakn.util.GraknTestUtil;
import ai.grakn.util.Schema;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;

import static ai.grakn.test.engine.postprocessing.PostProcessingTestUtils.createDuplicateResource;
import static ai.grakn.util.Schema.VertexProperty.INDEX;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class PostProcessingTest {
    private static final ObjectMapper mapper = new ObjectMapper();
    private PostProcessor postProcessor;
    private GraknSession session;

    @ClassRule
    public static final EngineContext engine = EngineContext.createWithInMemoryRedis();

    @BeforeClass
    public static void onlyRunOnTinker(){
        assumeTrue(GraknTestUtil.usingTinker());
    }

    @Before
    public void setUp() throws Exception {
        session = engine.sessionWithNewKeyspace();
        postProcessor = PostProcessor.create(engine.config(), engine.getJedisPool(), engine.server().factory(), engine.server().lockProvider(), new MetricRegistry());
    }

    @After
    public void takeDown() throws InterruptedException {
        session.close();
    }

    @Test
    public void whenCreatingDuplicateResources_EnsureTheyAreMergedInPost() throws InvalidKBException, InterruptedException, JsonProcessingException {
        String value = "1";
        String sample = "Sample";

        //Create GraknTx With Duplicate Resources
        GraknTx tx = session.open(GraknTxType.WRITE);
        AttributeType<String> attributeType = tx.putAttributeType(sample, AttributeType.DataType.STRING);

        Attribute<String> attribute = attributeType.putAttribute(value);
        tx.admin().commitSubmitNoLogs();
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

        // Casting sets as ConceptIds
        Set<ConceptId> resourceConcepts = merged.stream().map(c -> ConceptId.of(Schema.PREFIX_VERTEX + c.id().toString())).collect(toSet());

        //Create Commit Log
        CommitLog commitLog = CommitLog.createDefault(tx.keyspace());
        commitLog.attributes().put(resourceIndex, resourceConcepts);

        //Now fix everything
        PostProcessingTask task = new PostProcessingTask();
        //TaskConfiguration configuration = TaskConfiguration.of(mapper.writeValueAsString(commitLog));
        //task.initialize(configuration, engine.config(), engine.server().factory(),
        //        new MetricRegistry(), postProcessor);

        //task.start();

        tx = session.open(GraknTxType.READ);

        //Check it's fixed
        assertEquals(1, tx.getAttributeType(sample).instances().count());

        tx.close();
    }
}