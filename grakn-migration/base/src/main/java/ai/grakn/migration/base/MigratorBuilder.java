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
package ai.grakn.migration.base;

import ai.grakn.Keyspace;
import ai.grakn.client.GraknClient;
import ai.grakn.util.SimpleURI;

/**
 * <p>
 * Builder for the migrator.
 * </p>
 *
 * @author Domenico Corapi
 */
public class MigratorBuilder {
    private static final int DEFAULT_RETRIES = GraknClient.DEFAULT_MAX_RETRY;
    private static final boolean DEFAULT_FAIL_FAST = true;
    private static final int DEFAULT_MAX_DELAY_MS = 500;
    private static final int DEFAULT_LINES = -1;

    private SimpleURI uri;
    private Keyspace keyspace;
    private MigrationOptions migrationOptions = null;

    private int retries = DEFAULT_RETRIES;
    private boolean failFast = DEFAULT_FAIL_FAST;
    private int maxDelayMs = DEFAULT_MAX_DELAY_MS;
    private int lines = DEFAULT_LINES;


    public MigratorBuilder setUri(SimpleURI uri) {
        this.uri = uri;
        return this;
    }

    public MigratorBuilder setKeyspace(Keyspace keyspace) {
        this.keyspace = keyspace;
        return this;
    }

    public MigratorBuilder setMigrationOptions(MigrationOptions migrationOptions) {
        this.migrationOptions = migrationOptions;
        return this;
    }

    public MigratorBuilder setRetries(int retries) {
        this.retries = retries;
        return this;
    }

    public MigratorBuilder setFailFast(boolean failFast) {
        this.failFast = failFast;
        return this;
    }

    public MigratorBuilder setLines(int lines) {
        this.lines = lines;
        return this;
    }

    public Migrator build() {
        if (migrationOptions != null) {
            retries = migrationOptions.getRetry();
            maxDelayMs = migrationOptions.getMaxDelay();
            failFast = migrationOptions.isDebug();
            lines = migrationOptions.getLines();
        }
        return new Migrator(uri, keyspace, retries, failFast, maxDelayMs, lines);
    }
}