/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.graph.diskstorage.configuration.backend.builder;

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.diskstorage.configuration.backend.KCVSConfiguration;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import grakn.core.graph.diskstorage.util.BackendOperation;
import grakn.core.graph.diskstorage.util.time.TimestampProviders;

import java.time.Duration;

import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.SETUP_WAITTIME;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.SYSTEM_CONFIGURATION_IDENTIFIER;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.TIMESTAMP_PROVIDER;

/**
 * Builder to build KCVSConfiguration instances:
 * <p>
 * All KCVSConfigurations read from 'system_properties' Store
 */
public class KCVSConfigurationBuilder {

    /**
     * Build KCVSConfiguration concerning Global Configuration, system wide
     */
    public KCVSConfiguration buildGlobalConfiguration(BackendOperation.TransactionalProvider txProvider, KeyColumnValueStore store, Configuration config) {
        Duration setUpWaitingTime = config.get(SETUP_WAITTIME);
        TimestampProviders timestampProvider = config.get(TIMESTAMP_PROVIDER);
        Preconditions.checkArgument(Duration.ZERO.compareTo(setUpWaitingTime) < 0, "Wait time must be nonnegative: %s", setUpWaitingTime);
        return new KCVSConfiguration(txProvider, timestampProvider, setUpWaitingTime, store, SYSTEM_CONFIGURATION_IDENTIFIER);
    }
}
