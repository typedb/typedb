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

package grakn.core.graph.core.log;

import java.time.Instant;

/**
 * Builder for assembling a processor that processes a particular transaction LOG. A processor can be composed of one or multiple
 * {@link ChangeProcessor}s which are executed independently.
 */
public interface LogProcessorBuilder {

    /**
     * Returns the identifier of the transaction LOG to be processed by this processor.
     *
     * @return
     */
    String getLogIdentifier();

    /**
     * Sets the identifier of this processor. This String should uniquely identify a LOG processing instance and will be used to record
     * up to which position in the LOG the LOG processor has advanced. In case of instance failure or instance restart,
     * the LOG processor can then pick up where it left of.
     * <p>
     * This is an optional argument if recording the processing state is desired.
     *
     * @param name
     * @return
     */
    LogProcessorBuilder setProcessorIdentifier(String name);

    /**
     * Sets the time at which this LOG processor should start processing transaction LOG entries
     *
     * @param startTime
     * @return
     */
    LogProcessorBuilder setStartTime(Instant startTime);

    /**
     * Indicates that the transaction LOG processor should process newly added events.
     *
     * @return
     */
    LogProcessorBuilder setStartTimeNow();

    /**
     * Adds a {@link ChangeProcessor} to this transaction LOG processor. These are executed independently.
     *
     * @param processor
     * @return
     */
    LogProcessorBuilder addProcessor(ChangeProcessor processor);

    /**
     * Sets how often this LOG processor should attempt to retry executing a contained {@link ChangeProcessor} in case of failure.
     *
     * @param attempts
     * @return
     */
    LogProcessorBuilder setRetryAttempts(int attempts);

    /**
     * Builds this transaction LOG processor and starts processing the LOG.
     */
    void build();

}
