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

package ai.grakn.factory;

import ai.grakn.Grakn;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.exception.GraknTxOperationException;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FactoryBuilderTest {
    private final static GraknSession session = mock(GraknSession.class);
    private final static String TEST_CONFIG = "../conf/test/tinker/grakn.properties";
    private final static Keyspace KEYSPACE = Keyspace.of("keyspace");
    private final static String ENGINE_URL = Grakn.IN_MEMORY;
    private final static Properties TEST_PROPERTIES = new Properties();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void setup(){
        try (InputStream in = new FileInputStream(TEST_CONFIG)){
            TEST_PROPERTIES.load(in);
        } catch (IOException e) {
            throw GraknTxOperationException.invalidConfig(TEST_CONFIG);
        }
        when(session.config()).thenReturn(TEST_PROPERTIES);
    }

    @Test
    public void whenBuildingInMemoryFactory_ReturnTinkerFactory(){
        assertThat(FactoryBuilder.getFactory(session, false), instanceOf(TxFactoryTinker.class));
    }

    @Test
    public void whenBuildingFactoriesWithTheSameProperties_ReturnSameGraphs(){
        //Factory 1 & 2 Definition
        when(session.keyspace()).thenReturn(KEYSPACE);
        when(session.uri()).thenReturn(ENGINE_URL);
        when(session.config()).thenReturn(TEST_PROPERTIES);
        TxFactory mgf1 = FactoryBuilder.getFactory(session, false);
        TxFactory mgf2 = FactoryBuilder.getFactory(session, false);

        //Factory 3 & 4
        when(session.keyspace()).thenReturn(Keyspace.of("key"));
        TxFactory mgf3 = FactoryBuilder.getFactory(session, false);
        TxFactory mgf4 = FactoryBuilder.getFactory(session, false);

        assertEquals(mgf1, mgf2);
        assertEquals(mgf3, mgf4);
        assertNotEquals(mgf1, mgf3);

        assertNotEquals(mgf1.open(GraknTxType.WRITE), mgf3.open(GraknTxType.WRITE));
    }
}