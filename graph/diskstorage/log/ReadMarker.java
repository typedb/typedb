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

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.util.time.TimestampProvider;

import java.time.Instant;


public class ReadMarker {

    private final String identifier;
    private Instant startTime;

    private ReadMarker(String identifier, Instant startTime) {
        this.identifier = identifier;
        this.startTime = startTime;
    }

    /**
     * Whether this read marker has a configured identifier
     */
    public boolean hasIdentifier() {
        return identifier != null;
    }

    /**
     * Returns the configured identifier of this marker or throws an exception if none exists.
     */
    public String getIdentifier() {
        Preconditions.checkArgument(identifier != null, "ReadMarker does not have a configured identifier");
        return identifier;
    }

    public boolean hasStartTime() {
        return startTime != null;
    }

    /**
     * Returns the start time of this marker if such has been defined or the current time if not
     */
    public synchronized Instant getStartTime(TimestampProvider times) {
        if (startTime == null) {
            startTime = times.getTime();
        }
        return startTime;
    }

    public boolean isCompatible(ReadMarker newMarker) {
        if (newMarker.hasIdentifier()) {
            return hasIdentifier() && identifier.equals(newMarker.identifier);
        }
        return !newMarker.hasStartTime();
    }

    /**
     * Starts reading the LOG such that it will start with the first entry written after now.
     */
    public static ReadMarker fromNow() {
        return new ReadMarker(null, null);
    }

    /**
     * Starts reading the LOG from the given timestamp onward. The specified timestamp is included.
     */
    public static ReadMarker fromTime(Instant timestamp) {
        return new ReadMarker(null, timestamp);
    }

    /**
     * Starts reading the LOG from the last recorded point in the LOG for the given id.
     * If the LOG has a record of such an id, it will use it as the starting point.
     * If not, it will start from the given timestamp and set it as the first read record for the given id.
     * <p>
     * Identified read markers of this kind are useful to continuously read from the LOG. In the case of failure,
     * the last read record can be recovered for the id and LOG reading can be resumed from there. Note, that some
     * records might be read twice in that event depending on the guarantees made by a particular implementation.
     */
    public static ReadMarker fromIdentifierOrTime(String id, Instant timestamp) {
        return new ReadMarker(id, timestamp);
    }

    /**
     * Like #fromIdentifierOrTime(String id, Instant timestamp) but uses the current time point
     * as the starting timestamp if the LOG has no record of the id.
     */
    public static ReadMarker fromIdentifierOrNow(String id) {
        return new ReadMarker(id, Instant.EPOCH);
    }

}
