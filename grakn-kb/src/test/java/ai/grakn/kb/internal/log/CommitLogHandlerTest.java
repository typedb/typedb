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

package ai.grakn.kb.internal.log;

/*-
 * #%L
 * grakn-kb
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.Grakn;
import ai.grakn.Keyspace;
import org.junit.Test;

import java.net.URI;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

/**
 * @author Felix Chapman
 */
public class CommitLogHandlerTest {

    @Test
    public void callingGetCommitLogEndPointWithInMemory_ReturnsEmpty() {
        Optional<URI> endpoint = CommitLogHandler.getCommitLogEndPoint(Grakn.IN_MEMORY, Keyspace.of("whatever"));
        assertEquals(Optional.empty(), endpoint);
    }

    @Test
    public void callingGetCommitLogEndPointWithValidURI_ReturnsCorrectEndpoint() {
        Optional<URI> endpoint = CommitLogHandler.getCommitLogEndPoint("http://validuri.com:342", Keyspace.of("whatever"));
        assertEquals(Optional.of(URI.create("http://validuri.com:342/kb/whatever/commit_log")), endpoint);
    }

    @Test
    public void callingGetCommitLogEndPointWithURIMissingSchema_ReturnsCorrectEndpoint() {
        Optional<URI> endpoint = CommitLogHandler.getCommitLogEndPoint("validuri.com:342", Keyspace.of("whatever"));
        assertEquals(Optional.of(URI.create("http://validuri.com:342/kb/whatever/commit_log")), endpoint);
    }
}
