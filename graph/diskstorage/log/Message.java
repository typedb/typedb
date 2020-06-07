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

import grakn.core.graph.diskstorage.StaticBuffer;

import java.time.Instant;

/**
 * Messages which are added to and received from the Log.
 *
 */
public interface Message {

    /**
     * Returns the unique identifier for the sender of the message
     * @return
     */
    String getSenderId();

    /**
     * Returns the timestamp of this message in the specified time unit.
     * This is the time when the message was added to the LOG.
     * @return
     */
    Instant getTimestamp();

    /**
     * Returns the content of the message
     * @return
     */
    StaticBuffer getContent();

}
