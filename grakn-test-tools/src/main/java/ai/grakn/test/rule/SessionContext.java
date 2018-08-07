/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.test.rule;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.util.GraknTestUtil;
import com.google.common.collect.ImmutableList;
import org.junit.rules.TestRule;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static ai.grakn.util.SampleKBLoader.randomKeyspace;

/**
 * Context for tests that use {@link GraknTx}s and {@link ai.grakn.GraknSession}s.
 * Will make sure that any dependencies such as cassandra are running
 *
 * @author Felix Chapman
 */
public class SessionContext extends CompositeTestRule {
    private Set<GraknSession> openedSessions = new HashSet<>();

    private SessionContext() {
    }

    public static SessionContext create() {
        return new SessionContext();
    }

    @Override
    protected List<TestRule> testRules() {
        if (GraknTestUtil.usingJanus()) {
            return ImmutableList.of(EmbeddedCassandraContext.create());
        } else {
            return ImmutableList.of();
        }
    }

    public static boolean canUseTx() {
        return !GraknTestUtil.usingJanus() || EmbeddedCassandraContext.inCassandraContext();
    }

    public GraknSession newSession() {
        GraknSession session = (GraknTestUtil.usingJanus()) ? EmbeddedGraknSession.createEngineSession(randomKeyspace()) : EmbeddedGraknSession.inMemory(randomKeyspace());
        openedSessions.add(session);
        return session;
    }

    @Override
    public void after() {
        openedSessions.forEach(GraknSession::close);
    }
}
