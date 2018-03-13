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

package ai.grakn.remote;

import ai.grakn.GraknSession;
import ai.grakn.Keyspace;
import ai.grakn.util.SimpleURI;

/**
 * Entry-point and remote equivalent of {@link ai.grakn.Grakn}. Communicates with a running Grakn server using gRPC.
 *
 * <p>
 *     In the future, this will likely become the default entry-point over {@link ai.grakn.Grakn}. For now, only a
 *     subset of {@link GraknSession} and {@link ai.grakn.GraknTx} features are supported.
 * </p>
 *
 * @author Felix Chapman
 */
public final class RemoteGrakn {

    private RemoteGrakn() {}

    public static GraknSession session(SimpleURI uri, Keyspace keyspace) {
        return RemoteGraknSession.create(keyspace, uri);
    }
}
