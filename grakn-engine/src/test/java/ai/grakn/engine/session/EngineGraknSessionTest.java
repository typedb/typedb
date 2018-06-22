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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine.session;

import ai.grakn.Grakn;
import ai.grakn.GraknConfigKey;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.engine.GraknConfig;
import ai.grakn.engine.KeyspaceStore;
import ai.grakn.engine.KeyspaceStoreFake;
import ai.grakn.engine.ServerStatus;
import ai.grakn.engine.controller.SparkContext;
import ai.grakn.engine.controller.SystemController;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.JedisLockProvider;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.test.rule.InMemoryRedisContext;
import ai.grakn.test.rule.SessionContext;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.GraknTestUtil;
import ai.grakn.util.SampleKBLoader;
import ai.grakn.util.SimpleURI;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Iterables;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.mockito.Mockito.mock;

public class EngineGraknSessionTest {
    private static final GraknConfig config = GraknConfig.create();
    private static final ServerStatus status = mock(ServerStatus.class);
    private static final MetricRegistry metricRegistry = new MetricRegistry();
    private static final KeyspaceStoreFake systemKeyspace = KeyspaceStoreFake.of();

    private static EngineGraknTxFactory graknFactory;

    @ClassRule
    public static InMemoryRedisContext inMemoryRedisContext = InMemoryRedisContext.create(new SimpleURI(Iterables.getOnlyElement(config.getProperty(GraknConfigKey.REDIS_HOST))).getPort());

    //Needed so that Grakn.session() can return a session
    @ClassRule
    public static final SparkContext sparkContext = SparkContext.withControllers(new SystemController(config, systemKeyspace, status, metricRegistry));

    //Needed to start cass depending on profile
    @ClassRule
    public static final SessionContext sessionContext = SessionContext.create();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() {
        JedisLockProvider lockProvider = new JedisLockProvider(inMemoryRedisContext.jedisPool());
        KeyspaceStore keyspaceStore = KeyspaceStoreFake.of();
        graknFactory = EngineGraknTxFactory.create(lockProvider, config, keyspaceStore);
        graknFactory.keyspaceStore().loadSystemSchema();
    }

    @Test
    public void whenOpeningTransactionsOfTheSameKeyspaceFromSessionOrEngineFactory_EnsureTransactionsAreTheSame(){
        String keyspace = "mykeyspace";

        GraknTx tx1 = Grakn.session(sparkContext.uri(), keyspace).open(GraknTxType.WRITE);
        tx1.close();
        GraknTx tx2 = graknFactory.tx(Keyspace.of(keyspace), GraknTxType.WRITE);

        assertEquals(tx1, tx2);
        tx2.close();
    }

    @Test
    public void testBatchLoadingGraphsInitialisedCorrectly(){
        String keyspace = "mykeyspace";

        EmbeddedGraknTx<?> tx1 = graknFactory.tx(Keyspace.of(keyspace), GraknTxType.WRITE);
        tx1.close();
        EmbeddedGraknTx<?> tx2 = graknFactory.tx(Keyspace.of(keyspace), GraknTxType.BATCH);

        assertFalse(tx1.isBatchTx());
        assertTrue(tx2.isBatchTx());

        tx1.close();
        tx2.close();
    }

    @Test
    public void whenInsertingAfterSessionHasBeenClosed_shouldThrowTxException(){
        assumeFalse(GraknTestUtil.usingTinker()); //Tinker does not have any connections to close

        GraknSession session = Grakn.session(sparkContext.uri(), SampleKBLoader.randomKeyspace());
        GraknTx tx = session.open(GraknTxType.WRITE);
        session.close();

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(ErrorMessage.SESSION_CLOSED.getMessage(tx.keyspace()));

        tx.putEntityType("A thingy");
    }
}
