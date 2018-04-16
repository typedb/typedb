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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.factory;

import ai.grakn.Grakn;
import ai.grakn.Keyspace;
import ai.grakn.engine.GraknConfig;
import ai.grakn.kb.internal.log.CommitLogHandler;
import ai.grakn.test.rule.EmbeddedCassandraContext;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.nio.file.Paths;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class JanusTestBase {
    protected final static EmbeddedGraknSession session = mock(EmbeddedGraknSession.class);
    private final static File CONFIG_LOCATION = Paths.get("../conf/main/grakn.properties").toFile();
    private final static Keyspace TEST_SHARED = Keyspace.of("shared");
    private final static CommitLogHandler commitLogHandler = mock(CommitLogHandler.class);
    static TxFactoryJanus janusGraphFactory;
    final static GraknConfig TEST_CONFIG = GraknConfig.read(CONFIG_LOCATION);

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @ClassRule
    public static final EmbeddedCassandraContext cassandra = EmbeddedCassandraContext.create();

    @BeforeClass
    public static void setupMain(){
        when(session.keyspace()).thenReturn(TEST_SHARED);
        when(session.uri()).thenReturn(Grakn.IN_MEMORY);
        when(session.config()).thenReturn(TEST_CONFIG);
        when(session.commitLogHandler()).thenReturn(commitLogHandler);
        janusGraphFactory = new TxFactoryJanus(session);
    }

    TxFactoryJanus newFactory(){
        when(session.keyspace()).thenReturn(Keyspace.of("hoho" + UUID.randomUUID().toString().replace("-", "")));
        return new TxFactoryJanus(session);
    }
}
