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

package ai.grakn.engine;

import ai.grakn.Keyspace;
import ai.grakn.concept.Label;

import java.util.Set;

/**
 * <p>
 * Manages the system keyspace.
 * </p>
 *
 * <p>
 * Used to populate the system schema the first time the system keyspace
 * is created.
 * </p>
 *
 * <p>
 * Used to populate the system keyspace with all newly create keyspaces as a
 * user opens them. We have no way to determining whether a keyspace with a
 * given name already exists or not. We maintain the list in our Grakn system
 * keyspace. An element is added to that list when there is an attempt to create
 * a graph from a factory bound to the keyspace name. The list is simply the
 * instances of the system entity type 'keyspace'. Nothing is ever removed from
 * that list. The set of known keyspaces is maintained in a static map so we
 * don't connect to the system keyspace every time a factory produces a new
 * graph. That means that we can't have several different factories (e.g. Janus
 * and in-memory Tinkerpop) at the same time sharing keyspace names. We can't
 * identify the factory builder by engineUrl and config because we don't know
 * what's inside the config, which is residing remotely at the engine!
 * </p>
 *
 * @author borislav, fppt
 *
 */
public interface KeyspaceStore {
    // This will eventually be configurable and obtained the same way the factory is obtained
    // from engine. For now, we just make sure Engine and Core use the same system keyspace name.
    // If there is a more natural home for this constant, feel free to put it there! (Boris)
    Keyspace SYSTEM_KB_KEYSPACE = Keyspace.of("graknsystem");
    Label KEYSPACE_RESOURCE = Label.of("keyspace-name");

    /**
     * Checks if the keyspace exists in the system. The persisted graph is checked each time because the graph
     * may have been deleted in another JVM.
     *
     * @param keyspace The {@link Keyspace} which might be in the system
     * @return true if the keyspace is in the system
     */
    boolean containsKeyspace(Keyspace keyspace);

    /**
     * This removes the keyspace of the deleted graph from the system graph
     *
     * @param keyspace the {@link Keyspace} to be removed from the system graph
     *
     * @return if the keyspace existed
     */
    boolean deleteKeyspace(Keyspace keyspace);

    Set<Keyspace> keyspaces();

    /**
     * Load the system schema into a newly created system keyspace. Because the schema
     * only consists of types, the inserts are idempotent and it is safe to load it
     * multiple times.
     */
    void loadSystemSchema();

    void addKeyspace(Keyspace keyspace);
}
