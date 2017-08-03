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
package ai.grakn.util;

import com.github.zxl0714.redismock.RedisServer;
import org.junit.rules.ExternalResource;

/**
 * Rule class for executing tests that require a Redis mock
 *
 * @author pluraliseseverythings
 */
public class MockRedisRule extends ExternalResource {
    private RedisServer server;

    public MockRedisRule() {}

    @Override
    protected void before() throws Throwable {
        server = RedisServer.newRedisServer();
        server.start();
    }

    @Override
    protected void after() {
        if (server != null) {
            server.stop();
            server = null;
        }
    }

    public RedisServer getServer() {
        return server;
    }
}
