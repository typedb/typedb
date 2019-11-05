/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graph.diskstorage.locking;

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.util.time.TimestampProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A singleton maintaining a globally unique map of {@link LocalLockMediator}
 * instances.
 *
 * @see LocalLockMediatorProvider
 */
public enum LocalLockMediators implements LocalLockMediatorProvider {
    INSTANCE;

    private static final Logger LOG = LoggerFactory.getLogger(LocalLockMediators.class);

    /**
     * Maps a namespace to the mediator responsible for the namespace.
     * <p>
     * Implementation note: for Cassandra, namespace is usually a column
     * family name.
     */
    private final ConcurrentHashMap<String, LocalLockMediator<?>> mediators = new ConcurrentHashMap<>();

    @Override
    public <T> LocalLockMediator<T> get(String namespace, TimestampProvider times) {

        Preconditions.checkNotNull(namespace);

        @SuppressWarnings("unchecked")
        LocalLockMediator<T> m = (LocalLockMediator<T>) mediators.get(namespace);

        if (null == m) {
            m = new LocalLockMediator<>(namespace, times);
            @SuppressWarnings("unchecked") final LocalLockMediator<T> old = (LocalLockMediator<T>) mediators.putIfAbsent(namespace, m);
            if (null != old)
                m = old;
            else
                LOG.debug("Local lock mediator instantiated for namespace {}",
                        namespace);
        }

        return m;
    }

    /**
     * Only use this in testing.
     * <p>
     * This deletes the global map of namespaces to mediators. Calling this in
     * production would result in undetected locking failures and data
     * corruption.
     */
    public void clear() {
        mediators.clear();
    }

    /**
     * Only use this in testing.
     * <p>
     * This deletes all entries in the global map of namespaces to mediators
     * whose namespace key equals the argument.
     */
    public void clear(String namespace) {
        mediators.entrySet().removeIf(e -> e.getKey().equals(namespace));
    }
}
