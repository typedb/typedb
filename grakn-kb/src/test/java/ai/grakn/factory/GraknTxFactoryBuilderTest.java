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

package ai.grakn.factory;

import ai.grakn.Grakn;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.engine.GraknConfig;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GraknTxFactoryBuilderTest {
    private final static EmbeddedGraknSession session = mock(EmbeddedGraknSession.class);
    private final static File TEST_CONFIG_FILE = Paths.get("../conf/test/tinker/grakn.properties").toFile();
    private final static Keyspace KEYSPACE = Keyspace.of("keyspace");
    private final static String ENGINE_URL = Grakn.IN_MEMORY;
    private final static GraknConfig TEST_CONFIG = GraknConfig.read(TEST_CONFIG_FILE);

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void setup(){
        when(session.config()).thenReturn(TEST_CONFIG);
    }

    @Test
    public void whenBuildingInMemoryFactory_ReturnTinkerFactory(){
        assertThat(GraknTxFactoryBuilder.getInstance().getFactory(session, false), instanceOf(TxFactoryTinker.class));
    }

    @Test
    public void whenBuildingFactoriesWithTheSameProperties_ReturnSameGraphs(){
        //Factory 1 & 2 Definition
        when(session.keyspace()).thenReturn(KEYSPACE);
        when(session.uri()).thenReturn(ENGINE_URL);
        when(session.config()).thenReturn(TEST_CONFIG);
        TxFactory mgf1 = GraknTxFactoryBuilder.getInstance().getFactory(session, false);
        TxFactory mgf2 = GraknTxFactoryBuilder.getInstance().getFactory(session, false);

        //Factory 3 & 4
        when(session.keyspace()).thenReturn(Keyspace.of("key"));
        TxFactory mgf3 = GraknTxFactoryBuilder.getInstance().getFactory(session, false);
        TxFactory mgf4 = GraknTxFactoryBuilder.getInstance().getFactory(session, false);

        assertEquals(mgf1, mgf2);
        assertEquals(mgf3, mgf4);
        assertNotEquals(mgf1, mgf3);

        assertNotEquals(mgf1.open(GraknTxType.WRITE), mgf3.open(GraknTxType.WRITE));
    }

    @Test
    public void whenBuildingFactoriesPointingToTheSameKeyspace_EnsureSingleFactoryIsReturned() throws ExecutionException, InterruptedException {
        Set<Future> futures = new HashSet<>();
        Set<TxFactory> factories = new HashSet<>();
        ExecutorService pool = Executors.newFixedThreadPool(10);

        for(int i =0; i < 20; i ++){
            futures.add(pool.submit(() -> factories.add(GraknTxFactoryBuilder.getInstance().getFactory(session, false))));
        }

        for (Future future : futures) {
            future.get();
        }

        assertEquals(1, factories.size());
    }
}