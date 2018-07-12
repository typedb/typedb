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

package ai.grakn.kb.internal.log;

import ai.grakn.Grakn;
import ai.grakn.Keyspace;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertEquals;

/**
 * @author Grakn Warriors
 */
public class CommitLogHandlerTest {

    @Test
    public void callingGetCommitLogEndPointWithInMemory_ReturnsEmpty() {
        URI endpoint = CommitLogHandler.getCommitLogEndPoint(Grakn.IN_MEMORY, Keyspace.of("whatever"));
        assertEquals(null, endpoint);
    }

    @Test
    public void callingGetCommitLogEndPointWithValidURI_ReturnsCorrectEndpoint() {
        URI endpoint = CommitLogHandler.getCommitLogEndPoint("http://validuri.com:342", Keyspace.of("whatever"));
        assertEquals(URI.create("http://validuri.com:342/kb/whatever/commit_log"), endpoint);
    }

    @Test
    public void callingGetCommitLogEndPointWithURIMissingSchema_ReturnsCorrectEndpoint() {
        URI endpoint = CommitLogHandler.getCommitLogEndPoint("validuri.com:342", Keyspace.of("whatever"));
        assertEquals(URI.create("http://validuri.com:342/kb/whatever/commit_log"), endpoint);
    }
}