// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grakn.core.graph.core.log;

import org.janusgraph.core.log.ChangeProcessor;

import java.time.Instant;

/**
 * Builder for assembling a processor that processes a particular transaction LOG. A processor can be composed of one or multiple
 * {@link ChangeProcessor}s which are executed independently.
 *
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
    org.janusgraph.core.log.LogProcessorBuilder setProcessorIdentifier(String name);

    /**
     * Sets the time at which this LOG processor should start processing transaction LOG entries
     *
     * @param startTime
     * @return
     */
    org.janusgraph.core.log.LogProcessorBuilder setStartTime(Instant startTime);

    /**
     * Indicates that the transaction LOG processor should process newly added events.
     *
     * @return
     */
    org.janusgraph.core.log.LogProcessorBuilder setStartTimeNow();

    /**
     * Adds a {@link ChangeProcessor} to this transaction LOG processor. These are executed independently.
     * @param processor
     * @return
     */
    org.janusgraph.core.log.LogProcessorBuilder addProcessor(ChangeProcessor processor);

    /**
     * Sets how often this LOG processor should attempt to retry executing a contained {@link ChangeProcessor} in case of failure.
     * @param attempts
     * @return
     */
    org.janusgraph.core.log.LogProcessorBuilder setRetryAttempts(int attempts);

    /**
     * Builds this transaction LOG processor and starts processing the LOG.
     */
    void build();

}
