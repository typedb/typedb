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

package ai.grakn.graql.shell;

/*-
 * #%L
 * grakn-graql-shell
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
import ai.grakn.GraknConfigKey;
import ai.grakn.GraknSession;
import ai.grakn.Keyspace;
import ai.grakn.engine.GraknConfig;
import ai.grakn.remote.RemoteGrakn;
import ai.grakn.util.SimpleURI;
import jline.console.ConsoleReader;

/**
 *
 *  Implementation of SessionProvider interface - this is used by GraqlConsole to retrieve a GraknSession given terminal options
 *
 * @author marcoscoppetta
 */

public class GraknSessionProvider implements SessionProvider{

    private final GraknConfig config;

    public GraknSessionProvider(GraknConfig config) {
        this.config = config;
    }

    @Override
    public GraknSession getSession(GraqlShellOptions options, ConsoleReader console) {
        int defaultGrpcPort = config.getProperty(GraknConfigKey.GRPC_PORT);
        SimpleURI defaultGrpcUri = new SimpleURI(Grakn.DEFAULT_URI.getHost(), defaultGrpcPort);
        SimpleURI location = options.getUri();

        SimpleURI uri = location != null ? location : defaultGrpcUri;
        Keyspace keyspace = options.getKeyspace();

        return RemoteGrakn.session(uri, keyspace);
    }
}
