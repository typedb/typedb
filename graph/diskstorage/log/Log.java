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

package grakn.core.graph.diskstorage.log;

import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.StaticBuffer;

import java.util.concurrent.Future;

/**
 * Represents a LOG that allows content to be added to it in the form of messages and to
 * read messages and their content from the LOG via registered MessageReaders.
 */
public interface Log {

    /**
     * Attempts to add the given content to the LOG and returns a Future for this action.
     * <p>
     * If the LOG is configured for immediate sending, then any exception encountered during this process is thrown
     * by this method. Otherwise, encountered exceptions are attached to the returned future.
     */
    Future<Message> add(StaticBuffer content);

    /**
     * Attempts to add the given content to the LOG and returns a Future for this action.
     * In addition, a key is provided to signal the recipient of the LOG message in partitioned logging systems.
     * <p>
     * If the LOG is configured for immediate sending, then any exception encountered during this process is thrown
     * by this method. Otherwise, encountered exceptions are attached to the returned future.
     */
    Future<Message> add(StaticBuffer content, StaticBuffer key);

    /**
     * @param readMarker Indicates where to start reading from the LOG once message readers are registered
     * @param reader     The readers to register (all at once)
     * see #registerReaders(ReadMarker, Iterable)
     */
    void registerReader(ReadMarker readMarker, MessageReader... reader);

    /**
     * Registers the given readers with this LOG. These readers will be invoked for each newly read message from the LOG
     * starting at the point identified by the provided ReadMarker.
     * <p>
     * If no previous readers were registered, invoking this method triggers reader threads to be instantiated.
     * If readers have been previously registered, then the provided ReadMarker must be compatible with the
     * previous ReadMarker or an exception will be thrown.
     *
     * @param readMarker Indicates where to start reading from the LOG once message readers are registered
     * @param readers    The readers to register (all at once)
     */
    void registerReaders(ReadMarker readMarker, Iterable<MessageReader> readers);

    /**
     * Removes the given reader from the list of registered readers and returns whether this reader was registered in the
     * first place.
     * Note, that removing the last reader does not stop the reading process. Use #close() instead.
     *
     * @return true if this MessageReader was registered before and was successfully unregistered, else false
     */
    boolean unregisterReader(MessageReader reader);

    /**
     * Returns the name of this LOG
     */
    String getName();

    /**
     * Closes this LOG and stops the reading process.
     */
    void close() throws BackendException;

}
