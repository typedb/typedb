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
import ai.grakn.Keyspace;
import ai.grakn.test.rule.EmbeddedCassandraContext;
import ai.grakn.util.ErrorMessage;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class JanusTestBase {
    protected final static GraknSession session = mock(GraknSession.class);
    private final static String CONFIG_LOCATION = "../../conf/main/grakn.properties";
    private final static Keyspace TEST_SHARED = Keyspace.of("shared");
    static TxFactoryJanus janusGraphFactory;
    final static Properties TEST_PROPERTIES = new Properties();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @ClassRule
    public static final EmbeddedCassandraContext cassandra = EmbeddedCassandraContext.create();

    @BeforeClass
    public static void setupMain(){
        try (InputStream in = new FileInputStream(CONFIG_LOCATION)){
            TEST_PROPERTIES.load(in);
        } catch (IOException e) {
            throw new RuntimeException(ErrorMessage.INVALID_PATH_TO_CONFIG.getMessage(CONFIG_LOCATION), e);
        }

        when(session.keyspace()).thenReturn(TEST_SHARED);
        when(session.uri()).thenReturn(Grakn.IN_MEMORY);
        when(session.config()).thenReturn(TEST_PROPERTIES);
        janusGraphFactory = new TxFactoryJanus(session);
    }

    TxFactoryJanus newFactory(){
        when(session.keyspace()).thenReturn(Keyspace.of("hoho" + UUID.randomUUID().toString().replace("-", "")));
        return new TxFactoryJanus(session);
    }
}
